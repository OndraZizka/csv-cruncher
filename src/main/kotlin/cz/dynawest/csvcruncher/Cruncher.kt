package cz.dynawest.csvcruncher

import ch.qos.logback.classic.Level
import cz.dynawest.csvcruncher.HsqlDbHelper.Companion.quote
import cz.dynawest.csvcruncher.app.ExitCleanupStrategy
import cz.dynawest.csvcruncher.app.ExportArgument
import cz.dynawest.csvcruncher.app.Format
import cz.dynawest.csvcruncher.app.Options
import cz.dynawest.csvcruncher.app.OptionsEnums.CombineInputFiles
import cz.dynawest.csvcruncher.app.OptionsEnums.JsonExportFormat
import cz.dynawest.csvcruncher.converters.json.JsonFileFlattener
import cz.dynawest.csvcruncher.util.FilesUtils
import cz.dynawest.csvcruncher.util.HsqlDbTableCreator
import cz.dynawest.csvcruncher.util.HsqlDbTableCreator.ColumnInfo
import cz.dynawest.csvcruncher.util.JsonUtils
import cz.dynawest.csvcruncher.util.SqlFunctions.defineSqlFunctions
import cz.dynawest.csvcruncher.util.Utils
import cz.dynawest.csvcruncher.util.Utils.resolvePathToUserDirIfRelative
import cz.dynawest.csvcruncher.util.logger
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

class Cruncher(private val options: Options) {
    private lateinit var jdbcConn: Connection
    private lateinit var dbHelper: HsqlDbHelper
    private val log = logger()

    private fun init() {
        System.setProperty("hsqldb.method_class_names", "${javaClass.packageName}.*")
        System.setProperty("textdb.allow_full_path", "true")
        //System.setProperty("hsqldb.reconfig_logging", "false");
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver")
        } catch (e: ClassNotFoundException) {
            throw CsvCruncherException("Couldn't find JDBC driver: " + e.message, e)
        }

        val dbPath = StringUtils.defaultIfEmpty(options.dbPath, "hsqldb").toString() + "/cruncher"
        try {
            val existedBefore = File(dbPath).exists()
            FileUtils.forceMkdir(File(dbPath))
            if (options.dbDirOnExit == ExitCleanupStrategy.DELETE && !existedBefore)
                File(dbPath).deleteOnExit()
            jdbcConn = DriverManager.getConnection("jdbc:hsqldb:file:$dbPath;shutdown=true;sql.syntax_mys=true", "SA", "")
        }
        catch (e: IOException) {
            throw CsvCruncherException("Can't create HSQLDB data dir $dbPath: ${e.message}", e)
        }
        catch (e: SQLException) {
            throw CsvCruncherException("Can't connect to the database $dbPath: ${e.message}", e)
        }
        dbHelper = HsqlDbHelper(jdbcConn)
    }

    /**
     * Performs the whole process.
     */
    fun crunch() {
        try { options.validateAndApplyDefaults() }
        catch (ex: Exception) {
            throw CrucherConfigException("ERROR: Invalid options: ${ex.message}")
        }

        val addCounterColumn = options.initialRowNumber != null
        val tablesToFiles: MutableMap<String, File> = HashMap()
        var outputs: List<CruncherOutputPart> = emptyList()

        // Should the result have a unique incremental ID as an added 1st column?
        val counterColumn = CounterColumn()
        if (addCounterColumn) counterColumn.setDdlAndVal()
        try {
            dbHelper.executeSql("SET AUTOCOMMIT TRUE", "Error setting AUTOCOMMIT TRUE")
            defineSqlFunctions(dbHelper)

            for (script in options.initSqlArguments)
                dbHelper.executeSqlScript(script.path, "Error executing init SQL script")

            // Sort the input paths.
            var importArguments = options.importArguments.filter { it.path != null }
            importArguments = FilesUtils.sortImportsByPath(importArguments, options.sortInputPaths)
            log.debug(" --- Sorted imports: --- " + importArguments.joinToString { "\n * $it" })

            // Convert the .json files to .csv files.
            importArguments = importArguments.map { import ->
                if (!import.path!!.fileName.toString().endsWith(".json") || !import.path!!.toFile().isFile) {
                    import
                }
                else {
                    log.debug("Converting JSON to CSV: subtree ${import.itemsPathInTree} in ${import.path}")
                    val convertedFilePath = convertJsonToCsv(import.path!!, import.itemsPathInTree)
                    import.apply { path = convertedFilePath }  // Hack - replacing the path with the converted file.
                }
            }

            // A shortcut - for case of: crunch -in foo.json -out bar.csv, we are done.
            if (importArguments.size == 1 && options.exportArguments.size == 1){
                val singleConverted = options.importArguments.first()
                val singleExport = options.exportArguments.first()
                if (singleExport.sqlQuery == null && singleExport.formats == setOf(Format.CSV)) {
                    singleExport.path!!.toFile().mkdirs()
                    Files.move(singleConverted.path!!, singleExport.path!!)
                    return
                }
            }

            // Combine files. For cases like merging logs, or SQL WAL dumps.
            // Currently, this concats the files rather than UNIONing the tables.
            val inputSubparts: List<CruncherInputSubpart>
            if (options.combineInputFiles == CombineInputFiles.NONE) {
                inputSubparts = importArguments.map { import -> CruncherInputSubpart.trivial(import) } .toList()
            }
            else {
                // TBD: Apply some ImportArgument options (like indexed columns) if same.
                // TBD: Fail on unapplicable ImportArgument options.
                val inputFileGroups: Map<Path?, List<Path>> = FilesUtils.expandFilterSortInputFilesGroups(importArguments.map { it.path!! }, options)
                inputSubparts = FilesUtils.combineInputFiles(inputFileGroups, options)
                log.info(" --- Combined input files: --- " + inputSubparts.joinToString { p: CruncherInputSubpart -> "\n * ${p.combinedFile}" })
            }
            if (inputSubparts.isEmpty()) return
            FilesUtils.validateInputFiles(inputSubparts)

            // For each input CSV file...
            for (inputSubpart in inputSubparts) {

                val csvInFile = resolvePathToUserDirIfRelative(inputSubpart.combinedFile)
                log.info(" * Next CSV input: $inputSubpart")

                // Derive table name from the `-as` param, or the file name if missing.
                val tableName: String = HsqlDbHelper.normalizeTableName(inputSubpart.tableName)

                val previousIfAny = tablesToFiles.put(tableName, csvInFile)
                require(previousIfAny == null) { "Table name '$tableName' derived for 2 file names collide: $previousIfAny, $csvInFile" }

                val colNames: List<String> = FilesUtils.parseColumnsFromFirstCsvLine(csvInFile)
                // Create a table and bind the CSV to it.
                HsqlDbTableCreator(dbHelper).createTableFromInputFile(
                    tableName = tableName,
                    csvFileToBind = csvInFile,
                    columnNames = colNames,
                    colsForIndex = inputSubpart.originalImportArgument?.indexed ?: emptyList(),
                    ignoreFirst = true,
                    overwrite = options.overwrite
                )
                inputSubpart.tableName = tableName
            }


            // Perform the SQL SELECTs

            if (options.exportArguments.size > 1)
                throw UnsupportedOperationException("Currently, only 1 export is supported.")


            for (export in options.exportArguments) {

                outputs = mutableListOf()

                // SQL can be executed:
                // A) Once over all tables, and generate a single result.
                if (!options.queryPerInputSubpart) {
                    val csvOutFile = resolvePathToUserDirIfRelative(export.path!!)

                    // If there's just one input, then the generic SQL can be used.
                    outputs.add(
                        CruncherOutputPart(csvOutFile.toPath(), export, if (inputSubparts.size == 1) inputSubparts.first().tableName else null)
                    )
                }
                // B) With each table, with a generic SQL query (using "$table"), and generate one result per table.
                else {
                    val usedOutputFiles: MutableSet<Path> = HashSet()
                    for (inputSubpart in inputSubparts) {
                        var outputFile = export.path!!.resolve(inputSubpart.combinedFile.fileName)
                        outputFile = FilesUtils.getNonUsedName(outputFile, usedOutputFiles)
                        val output = CruncherOutputPart(outputFile, export, inputSubpart.tableName)
                        outputs.add(output)
                    }
                }

                val genericSql = dbHelper.quoteColumnAndTableNamesInQuery(export.sqlQuery ?: DEFAULT_SQL)

                // For each output...
                for (output in outputs) {
                    log.debug("Output part: {}", output)
                    val csvOutFile = output.outputFile.toFile()
                    val outputTableName = output.deriveOutputTableName()

                    var sql = genericSql
                    if (output.inputTableName != null) {
                        sql = sql.replace(SQL_TABLE_PLACEHOLDER, quote(output.inputTableName))
                    }
                    else if (export.sqlQuery == null) {
                        throw CsvCruncherException("Default SQL is used (no -sql set), but no single input table name " +
                                "was determined automatically for output: $output\nTherefore the table placeholder can't be replaced.")
                    }


                    // Create the parent dir.
                    val dirToCreate = csvOutFile.absoluteFile.parentFile
                    dirToCreate.mkdirs()

                    // Get the columns info: Perform the SQL, LIMIT 1.
                    val columnsDef: Map<String, String> = dbHelper.extractColumnsInfoFrom1LineSelect(sql)
                    output.columnNamesAndTypes = columnsDef


                    // Write the result into a CSV
                    log.info(" * CSV output: $csvOutFile")
                    HsqlDbTableCreator(dbHelper).createTableAndBindCsv(
                        tableName = outputTableName,
                        csvFileToBind = csvOutFile,
                        columnsDef.mapValues { ColumnInfo(typeDdl = it.value) },
                        ignoreFirst = true,
                        counterColumnDdl = counterColumn.ddl,
                        isInputTable = false,
                        overwrite = options.overwrite
                    )

                    var contentForStdout = csvOutFile

                    // TBD: i109 Analyze the SQL: "EXPLAIN PLAN FOR $SQL"

                    // TBD: The export SELECT could reference the counter column, like "SELECT @counter, foo FROM ..."
                    // On the other hand, that's too much space for the user to screw up. Let's force it:
                    val selectSql = sql.replace("SELECT ", "SELECT ${counterColumn.value} ")
                    output.sql = selectSql
                    val userSql = "INSERT INTO ${quote(outputTableName)} ($selectSql)"
                    log.debug(" * User's SQL: $userSql")

                    val rowsAffected = dbHelper.executeSql(userSql, "Error executing user SQL: ")
                    log.debug("Affected rows: $rowsAffected")


                    // Now let's convert it to JSON if necessary.
                    val convertResultToJson = options.jsonExportFormat != JsonExportFormat.NONE || output.forExport.formats.contains(Format.JSON)
                    if (convertResultToJson) {
                        var pathStr: String = csvOutFile.toPath().toString()
                        pathStr = StringUtils.removeEndIgnoreCase(pathStr, ".csv")
                        pathStr = StringUtils.appendIfMissing(pathStr, ".json")
                        val destJsonFile = Paths.get(pathStr)
                        log.info(" * JSON output: $destJsonFile")

                        jdbcConn.createStatement().use { statement2 ->
                            JsonUtils.convertResultToJson(
                                statement2.executeQuery("SELECT * FROM ${quote(outputTableName)}"),
                                destJsonFile,
                                options.jsonExportFormat == JsonExportFormat.ARRAY
                            )
                            if (!output.forExport.formats.contains(Format.CSV) && !options.keepWorkFiles) csvOutFile.deleteOnExit()
                        }

                        contentForStdout = destJsonFile.toFile()
                    }

                    // Print the result to STDOUT.
                    if (output.forExport.targetType == ExportArgument.TargetType.STDOUT) {
                        contentForStdout.inputStream().use { IOUtils.copy(it, System.out) }
                        System.out.println()
                        if (!options.keepWorkFiles) contentForStdout.deleteOnExit()
                    }
                }
            }
        }
        catch (ex: CsvCruncherException) {
            // On a known error, we will print the message, and can stop logging, to prevent repeating it in the log.
            Utils.setRootLoggerLevel(Level.ERROR)
            throw ex
        }
        catch (ex: Exception) {
            throw ex
        }
        finally {
            log.debug(" *** SHUTDOWN CLEANUP SEQUENCE ***")
            cleanUpInputOutputTables(tablesToFiles, outputs)
            dbHelper.executeSql("DROP SCHEMA PUBLIC CASCADE", "Failed to delete the database: ")
            jdbcConn.close()
            log.debug(" *** END SHUTDOWN CLEANUP SEQUENCE ***")
        }
    }

    private fun convertJsonToCsv(inputPath: Path, itemsAt: String): Path {
        return JsonFileFlattener().convert(inputPath, itemsAt)
    }

    private fun cleanUpInputOutputTables(inputTablesToFiles: Map<String, File>, outputs: List<CruncherOutputPart>) {
        // TBD: Implement a cleanup at start. https://github.com/OndraZizka/csv-cruncher/issues/18
        dbHelper.detachTables(inputTablesToFiles.keys, "Could not delete the input table: ")

        val outputTablesNames = outputs.map { outputPart -> outputPart.deriveOutputTableName() }.toSet()
        dbHelper.detachTables(outputTablesNames, "Could not delete the output table: ")
    }

    /**
     * @return The initial number to use for unique row IDs.
     * Takes the value from options, or generates from timestamp if not set.
     */
    private val initialNumber: Long
        get() = options.initialRowNumber?.takeIf { it != -1L }
            ?: System.currentTimeMillis() - TIMESTAMP_SUBTRACT

    /**
     * Information for the extra column used to add a unique id to each row.
     */
    private inner class CounterColumn {
        var ddl = ""
        var value = ""
        fun setDdlAndVal(): CounterColumn {
            val initialNumber = initialNumber
            var sql: String

            // Using an IDENTITY column which has an unnamed sequence?
            //ddl = "crunchCounter BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY";
            // ALTER TABLE output ALTER COLUMN crunchCounter RESTART WITH UNIX_MILLIS() - 1530000000000;
            // INSERT INTO otherTable VALUES (IDENTITY(), ...)

            // Or using a sequence?
            sql = "CREATE SEQUENCE IF NOT EXISTS crunchCounter AS BIGINT NO CYCLE" // MINVALUE 1 STARTS WITH <number>
            dbHelper.executeSql(sql, "Failed creating the counter sequence: ")
            sql = "ALTER SEQUENCE crunchCounter RESTART WITH $initialNumber"
            dbHelper.executeSql(sql, "Failed altering the counter sequence: ")

            // ... referencing it explicitly?
            //ddl = "crunchCounter BIGINT PRIMARY KEY, ";
            // INSERT INTO output VALUES (NEXT VALUE FOR crunchCounter, ...)
            //value = "NEXT VALUE FOR crunchCounter, ";

            // ... or using it through GENERATED BY?
            ddl = "crunchCounter BIGINT GENERATED BY DEFAULT AS SEQUENCE crunchCounter PRIMARY KEY, "
            //value = "DEFAULT, ";
            value = "NULL AS crunchCounter, "
            // INSERT INTO output (id, firstname, lastname) VALUES (DEFAULT, ...)
            // INSERT INTO otherTable VALUES (CURRENT VALUE FOR crunchCounter, ...)
            return this
        }
    }

    companion object {
        const val TABLE_NAME__OUTPUT = "output"
        const val TIMESTAMP_SUBTRACT = 1600000000000L // To make the unique ID a smaller number.
        const val FILENAME_SUFFIX_CSV = ".csv"
        const val SQL_TABLE_PLACEHOLDER = "\$table"
        const val DEFAULT_SQL = "SELECT $SQL_TABLE_PLACEHOLDER.* FROM $SQL_TABLE_PLACEHOLDER"
    }

    init { init() }
}
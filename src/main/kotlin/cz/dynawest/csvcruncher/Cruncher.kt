package cz.dynawest.csvcruncher

import cz.dynawest.csvcruncher.Options.CombineInputFiles
import cz.dynawest.csvcruncher.Options.JsonExportFormat
import cz.dynawest.csvcruncher.converters.JsonFileFlattener
import cz.dynawest.csvcruncher.util.FilesUtils
import cz.dynawest.csvcruncher.util.Utils.resolvePathToUserDirIfRelative
import cz.dynawest.csvcruncher.util.logger
import lombok.extern.slf4j.Slf4j
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import java.io.File
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.*
import java.util.regex.Pattern
import java.util.stream.Collectors

@Slf4j
class Cruncher(private val options: Options) {
    private var jdbcConn: Connection? = null
    private var dbHelper: HsqlDbHelper? = null
    private val log = logger()

    private fun init() {
        System.setProperty("textdb.allow_full_path", "true")
        //System.setProperty("hsqldb.reconfig_logging", "false");
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver")
        } catch (e: ClassNotFoundException) {
            throw CsvCruncherException("Couldn't find JDBC driver: " + e.message, e)
        }
        val dbPath = StringUtils.defaultIfEmpty(options.dbPath, "hsqldb").toString() + "/cruncher"
        try {
            FileUtils.forceMkdir(File(dbPath))
            jdbcConn = DriverManager.getConnection("jdbc:hsqldb:file:$dbPath;shutdown=true", "SA", "")
        } catch (e: IOException) {
            throw CsvCruncherException(String.format("Can't create HSQLDB data dir %s: %s", dbPath, e.message), e)
        } catch (e: SQLException) {
            throw CsvCruncherException(String.format("Can't connect to the database %s: %s", dbPath, e.message), e)
        }
        dbHelper = HsqlDbHelper(jdbcConn)
    }

    /**
     * Performs the whole process.
     */
    @Throws(Exception::class)
    fun crunch() {
        options.validate()
        val addCounterColumn = options.initialRowNumber != null
        val convertResultToJson = options.jsonExportFormat != JsonExportFormat.NONE
        val printAsArray = options.jsonExportFormat == JsonExportFormat.ARRAY
        val tablesToFiles: MutableMap<String?, File> = HashMap()
        var outputs: List<CruncherOutputPart> = emptyList()

        // Should the result have a unique incremental ID as an added 1st column?
        val counterColumn = CounterColumn()
        if (addCounterColumn) counterColumn.setDdlAndVal()
        try {
            // Sort the input paths.
            var inputPaths = options.inputPaths!!.stream().map { first: String? -> Paths.get(first) }.collect(Collectors.toList())
            inputPaths = FilesUtils.sortInputPaths(inputPaths, options.sortInputPaths)
            log.debug(" --- Sorted input paths: --- " + inputPaths.stream().map { p: Path -> "\n * $p" }.reduce { obj: String, str: String -> obj + str }.get())

            // TODO: Convert the .json files to .csv files.
            inputPaths.map { inputPath ->
                if (inputPath.endsWith(".json"))

                    convertJsonToCsv(inputPath)
                else inputPath
            }

            val inputSubparts: List<CruncherInputSubpart>

            // Combine files. Should we concat the files or UNION the tables?
            if (options.combineInputFiles != CombineInputFiles.NONE) {
                val inputFileGroups: Map<Path?, List<Path>> = FilesUtils.expandFilterSortInputFilesGroups(inputPaths, options)

                ///Map<Path, List<Path>> resultingFilePathToConcatenatedFiles = FilesUtils.combineInputFiles(inputFileGroups, this.options);
                inputSubparts = FilesUtils.combineInputFiles(inputFileGroups, options)
                ///inputPaths = new ArrayList<>(inputSubparts.stream().map(x -> x.getCombinedFile()).collect(Collectors.toList()));
                ///log.info(" --- Combined input files: --- " + inputPaths.stream().map(p -> "\n * " + p).reduce(String::concat).orElse("NONE"));
                log.info(" --- Combined input files: --- " + inputSubparts.stream().map { p: CruncherInputSubpart ->
                    """
 * ${p.combinedFile}"""
                }.reduce { obj: String, str: String -> obj + str }.orElse("NONE"))
            } else {
                inputSubparts = inputPaths.stream().map { path: Path -> CruncherInputSubpart.trivial(path) }.collect(Collectors.toList())
                //inputPaths = new ArrayList<>(inputSubparts.stream().map(x -> x.getCombinedFile()).collect(Collectors.toList()));
            }
            if (inputSubparts.isEmpty()) return
            FilesUtils.validateInputFiles(inputSubparts)

            // For each input CSV file...
            for (inputSubpart in inputSubparts) {
                val csvInFile = resolvePathToUserDirIfRelative(inputSubpart.combinedFile)
                log.info(" * CSV input: $csvInFile")
                val tableName: String = HsqlDbHelper.Companion.normalizeFileNameForTableName(csvInFile)
                val previousIfAny = tablesToFiles.put(tableName, csvInFile)
                require(previousIfAny == null) { "File names normalized to table names collide: $previousIfAny, $csvInFile" }
                val colNames: List<String> = FilesUtils.parseColumnsFromFirstCsvLine(csvInFile)
                // Create a table and bind the CSV to it.
                dbHelper!!.createTableForInputFile(tableName, csvInFile, colNames, true, options.overwrite)
                inputSubpart.tableName = tableName
            }
            val genericSql = StringUtils.defaultString(options.sql, DEFAULT_SQL)
            outputs = ArrayList()


            // SQL can be executed:
            // * for all tables, and generate a single result; if some table has changed, it would fail.
            if (!options.queryPerInputSubpart) {
                val csvOutFile = resolvePathToUserDirIfRelative(Paths.get(options.outputPathCsv))
                val output = CruncherOutputPart(csvOutFile.toPath(), null)
                outputs.add(output)
            } else {
                val usedOutputFiles: MutableSet<Path> = HashSet()
                for (inputSubpart in inputSubparts) {
                    var outputFile = Paths.get(options.outputPathCsv).resolve(inputSubpart.combinedFile.fileName)
                    outputFile = FilesUtils.test_getNonUsedName(outputFile, usedOutputFiles)
                    val output = CruncherOutputPart(outputFile, inputSubpart.tableName)
                    outputs.add(output)
                }
            }


            // TODO: For each output...
            for (output in outputs) {
                log.debug("Output part: {}", output)
                val csvOutFile = output.outputFile.toFile()
                var sql = genericSql
                //String outputTableName = TABLE_NAME__OUTPUT;
                val outputTableName = output.deriveOutputTableName()
                if (output.inputTableName != null) {
                    sql = sql.replace(SQL_TABLE_PLACEHOLDER, output.inputTableName)
                    //outputTableName = output.getInputTableName() + "_out";
                }


                // Create the parent dir.
                val dirToCreate = csvOutFile.absoluteFile.parentFile
                dirToCreate.mkdirs()

                // Get the columns info: Perform the SQL, LIMIT 1.
                val columnsDef: Map<String, String> = dbHelper!!.extractColumnsInfoFrom1LineSelect(sql)
                output.columnNamesAndTypes = columnsDef


                // Write the result into a CSV
                log.info(" * CSV output: $csvOutFile")
                dbHelper!!.createTableAndBindCsv(outputTableName, csvOutFile, columnsDef, true, counterColumn.ddl, false, options.overwrite)

                // The provided SQL could be something like "SELECT @counter, foo, bar FROM ..."
                //String selectSql = this.options.sql.replace("@counter", value);
                // On the other hand, that's too much space for the user to screw up. Let's force it:
                val selectSql = sql.replace("SELECT ", "SELECT " + counterColumn.value + " ")
                output.sql = selectSql
                val userSql = "INSERT INTO $outputTableName ($selectSql)"
                log.debug(" * User's SQL: $userSql")
                //log.info("\n  Tables and column types:\n" + this.formatListOfAvailableTables(true));///
                val rowsAffected = dbHelper!!.executeDbCommand(userSql, "Error executing user SQL: ")


                // Now let's convert it to JSON if necessary.
                if (convertResultToJson) {
                    var pathStr: String? = csvOutFile.toPath().toString()
                    pathStr = StringUtils.removeEndIgnoreCase(pathStr, ".csv")
                    pathStr = StringUtils.appendIfMissing(pathStr, ".json")
                    val destJsonFile = Paths.get(pathStr)
                    log.info(" * JSON output: $destJsonFile")
                    jdbcConn!!.createStatement().use { statement2 ->
                        FilesUtils.convertResultToJson(
                                statement2.executeQuery("SELECT * FROM $outputTableName"),
                                destJsonFile,
                                printAsArray
                        )
                        if (!options.keepWorkFiles) csvOutFile.deleteOnExit()
                    }
                }
            }
        } catch (ex: Exception) {
            //throw new CsvCruncherException("(DB tables and files cleanup was performed.)", ex);
            throw ex // The wrapper above was more confusing than helping.
        } finally {
            log.debug(" *** SHUTDOWN CLEANUP SEQUENCE ***")
            cleanUpInputOutputTables(tablesToFiles, outputs)
            dbHelper!!.executeDbCommand("DROP SCHEMA PUBLIC CASCADE", "Failed to delete the database: ")
            jdbcConn!!.close()
            log.debug(" *** END SHUTDOWN CLEANUP SEQUENCE ***")
        }
    }

    private fun convertJsonToCsv(inputPath: Path): Path {
        return JsonFileFlattener().convert(inputPath)
    }

    private fun cleanUpInputOutputTables(inputTablesToFiles: Map<String?, File>, outputs: List<CruncherOutputPart>) {
        // TODO: Implement a cleanup at start. https://github.com/OndraZizka/csv-cruncher/issues/18
        dbHelper!!.detachTables(inputTablesToFiles.keys, "Could not delete the input table: ")

        //dbHelper.detachTables(Collections.singleton(TABLE_NAME__OUTPUT), "Could not delete the output table: ");
        val outputTablesNames = outputs.stream().map { x: CruncherOutputPart -> x.deriveOutputTableName() }.collect(Collectors.toSet())
        dbHelper!!.detachTables(outputTablesNames, "Could not delete the output table: ")
    }

    // A timestamp at the beginning:
    //sql = "DECLARE crunchCounter BIGINT DEFAULT UNIX_MILLIS() - 1530000000000";
    //executeDbCommand(sql, "Failed creating the counter variable: ");
    // Uh oh. Variables can't be used in SELECTs.

    /**
     * @return The initial number to use for unique row IDs.
     * Takes the value from options, or generates from timestamp if not set.
     */
    private val initialNumber: Long
        get() {
            val initialNumber: Long
            initialNumber = if (options.initialRowNumber != -1L) {
                options.initialRowNumber!!
            } else {
                // A timestamp at the beginning:
                //sql = "DECLARE crunchCounter BIGINT DEFAULT UNIX_MILLIS() - 1530000000000";
                //executeDbCommand(sql, "Failed creating the counter variable: ");
                // Uh oh. Variables can't be used in SELECTs.
                System.currentTimeMillis() - TIMESTAMP_SUBSTRACT
            }
            return initialNumber
        }

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
            dbHelper!!.executeDbCommand(sql, "Failed creating the counter sequence: ")
            sql = "ALTER SEQUENCE crunchCounter RESTART WITH $initialNumber"
            dbHelper!!.executeDbCommand(sql, "Failed altering the counter sequence: ")

            // ... referencing it explicitely?
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
        const val TIMESTAMP_SUBSTRACT = 1530000000000L // To make the unique ID a smaller number.
        const val FILENAME_SUFFIX_CSV = ".csv"
        val REGEX_SQL_COLUMN_VALID_NAME = Pattern.compile("[a-z][a-z0-9_]*", Pattern.CASE_INSENSITIVE)
        const val SQL_TABLE_PLACEHOLDER = "\$table"
        const val DEFAULT_SQL = "SELECT " + SQL_TABLE_PLACEHOLDER + ".* FROM " + SQL_TABLE_PLACEHOLDER
    }

    init {
        init()
    }
}
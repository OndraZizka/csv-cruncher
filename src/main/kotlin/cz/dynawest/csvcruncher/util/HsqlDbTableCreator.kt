package cz.dynawest.csvcruncher.util

import cz.dynawest.csvcruncher.CrucherConfigException
import cz.dynawest.csvcruncher.CsvCruncherException
import cz.dynawest.csvcruncher.HsqlDbHelper
import cz.dynawest.csvcruncher.HsqlDbHelper.Companion.quote
import org.apache.commons.lang3.RandomStringUtils
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.sql.SQLException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME
import kotlin.random.Random

class HsqlDbTableCreator(private val hsqlDbHelper: HsqlDbHelper) {


    @Throws(SQLException::class)
    fun createTableFromInputFile(tableName: String, csvFileToBind: File, columnNames: List<String>, colsForIndex: List<String>, ignoreFirst: Boolean, overwrite: Boolean) {
        val colGrp = columnNames.groupingBy { it }.eachCount()
        colGrp.filter { it.value != 1 }.takeIf { it.isNotEmpty() }
            ?.let { throw CrucherConfigException("Duplicate column names: " + it.entries.joinToString(", ") { "${it.key} ${it.value}x" }) }

        val indexedColumns = translatePositionsToNames(columnNames, colsForIndex)

        createTableAndBindCsv(tableName, csvFileToBind, columnNames, indexedColumns, ignoreFirst, "", true, overwrite)
    }

    private fun translatePositionsToNames(columnNames: List<String>, indexedColumns: List<String>): List<String> {
        //val namesToPosition = columnNames.mapIndexed { index, s -> Pair(s, index) }.associate { it.first to it.second }
        val problems = mutableListOf<String>()

        // From positions or names into positions.
        val positions: List<String?> = indexedColumns.map { col ->

            col.toIntOrNull()
                ?.let {
                    if (it !in 1..columnNames.size) {
                        problems += "The column position '$it' requested to be indexed is out of bounds 1..${columnNames.size}"
                        null
                    } else columnNames[it-1]
                }
                ?: col.takeIf { it.isNotBlank() }
                ?: let { problems += "One of the column positions requested to be indexed is blank: ${indexedColumns.joinToString(",")}"; null }
        }
        if (problems.isNotEmpty())
            throw CrucherConfigException(problems.joinToString("\n"))

        return positions.filterNotNull()
    }

    /**
     * Input tables columns are optimized after binding the file by attempting to reduce the column type.
     * (The output table has to be optimized later.)
     */
    @Suppress("SameParameterValue")
    @Throws(SQLException::class)
    private fun createTableAndBindCsv(
        tableName: String,
        csvFileToBind: File,
        columnsNames: List<String>,
        indexedColumns: List<String>,
        ignoreFirst: Boolean,
        counterColumnDdl: String,
        isInputTable: Boolean,
        overwrite: Boolean
    ) {
        val columnsInfos = columnsNames.associateWith { ColumnInfo(indexed = indexedColumns.contains(it)) }
        createTableAndBindCsv(tableName, csvFileToBind, columnsInfos, ignoreFirst, counterColumnDdl, isInputTable, overwrite)

        // Try to convert columns types to numbers, where applicable.
        if (isInputTable) {
            SqlTypeReducer(hsqlDbHelper)
                .optimizeTableColumnsType(tableName, columnsNames)
        }
    }

    data class ColumnInfo (
        val typeDdl: String? = null,
        val indexed: Boolean = false,
    )

    /**
     * Creates the input or output table, with the right column names, and binds the file.
     * For output tables, the file is optionally overwritten if exists.
     * A header with columns names is added to the output table.
     */
    @Throws(SQLException::class)
    fun createTableAndBindCsv(tableName: String, csvFileToBind: File, columnsInfo: Map<String, ColumnInfo>, ignoreFirst: Boolean, counterColumnDdl: String?, isInputTable: Boolean, overwrite: Boolean) {
        @Suppress("NAME_SHADOWING")
        var csvFileToBind = csvFileToBind
        val readOnly = false
        val csvUsesSingleQuote = true
        require(!(isInputTable && !csvFileToBind.exists())) { "The input file does not exist: $csvFileToBind" }

        // Get a full path, because HSQLDB resolves paths against the data dir specified in JDBC URL.
        csvFileToBind = try {
            csvFileToBind.canonicalFile
        } catch (ex: IOException) {
            throw CsvCruncherException("Failed resolving the CSV file path: $csvFileToBind", ex)
        }

        // Delete any file at the output path, if exists. Other option would be to TRUNCATE, but this is safer.
        if (!isInputTable) {
            if (csvFileToBind.exists()) {
                if (csvFileToBind.isDirectory)
                    throw IllegalArgumentException("The output destination is an existing directory, can't overwrite: $csvFileToBind")

                try {
                    if (overwrite)
                        csvFileToBind.delete()
                    else {
                        //throw IllegalArgumentException("The output file already exists. Use --overwrite or delete: $csvFileToBind")
                        val backupName = csvFileToBind.name + formatBackupSuffix()
                        log.info("Output already exists, renaming to: $backupName")
                        csvFileToBind.renameTo(File(backupName))
                    }
                }
                catch (ex: Exception) { throw CsvCruncherException("Failed dealing with an existing file in place of the output: $csvFileToBind\n${ex.message}", ex) }
            }
            else {
                try {
                    Files.createDirectories(csvFileToBind.parentFile.toPath())
                } catch (ex: IOException) {
                    throw CsvCruncherException("Failed creating directory to store the output to: " + csvFileToBind.parentFile, ex)
                }
            }
        }


        // We are also building a header for the CSV file.
        val sbCsvHeader = StringBuilder("# ")
        val sbSql = StringBuilder("CREATE TEXT TABLE ").append(quote(tableName)).append(" ( ")

        // The counter column, if any.
        sbSql.append(counterColumnDdl)

        // Columns
        for (columnDef in columnsInfo.entries) {
            sbCsvHeader.append(columnDef.key).append(", ")

            val columnQuotedName = quote(columnDef.key)

            var columnType = columnDef.value.typeDdl
            columnType =
                if (columnType == null || "VARCHAR" == columnType.uppercase())
                    "VARCHAR(${HsqlDbHelper.MAX_STRING_COLUMN_LENGTH})"
                else Utils.escapeSql(columnType)
            sbSql.append(columnQuotedName).append(" ").append(columnType).append(", ")
        }
        sbCsvHeader.delete(sbCsvHeader.length - 2, sbCsvHeader.length)
        sbSql.delete(sbSql.length - 2, sbSql.length)
        sbSql.append(" )")
        log.debug("Table DDL SQL: $sbSql")
        hsqlDbHelper.executeSql(sbSql.toString(), "Failed to CREATE TEXT TABLE: ")

        // Indexes. The ADD INDEX needs to be done before SET SOURCE.
        setUpTableIndexes(tableName, columnsInfo.filter { it.value.indexed }.map { it.key })

        // Bind the table to the CSV file.
        var csvPath = csvFileToBind.path
        csvPath = Utils.escapeSql(csvPath)
        val quoteCharacter = if (csvUsesSingleQuote) "\\quote" else "\""
        val ignoreFirstFlag = if (ignoreFirst) "ignore_first=true;" else ""
        val csvSettings = "encoding=UTF-8;cache_rows=50000;cache_size=10240000;" + ignoreFirstFlag + "fs=,;qc=" + quoteCharacter
        val DESC = if (readOnly) "DESC" else "" // Not a mistake, HSQLDB really has "DESC" here for read only.
        val sql = "SET TABLE ${quote(tableName)} SOURCE '$csvPath;$csvSettings' $DESC"
        log.debug("CSV import SQL: $sql")
        hsqlDbHelper.executeSql(sql, "Failed to import CSV: ")

        // SET TABLE <table name> SOURCE HEADER
        if (!isInputTable) {
            val sqlBind = "SET TABLE ${quote(tableName)} SOURCE HEADER '$sbCsvHeader'"
            log.debug("CSV source header SQL: $sqlBind")
            hsqlDbHelper.executeSql(sqlBind, "Failed to set CSV header: ")
        }
    }

    private fun formatBackupSuffix() = ".backup-" +
            LocalDateTime.now().format(ISO_LOCAL_DATE_TIME) + "-" +
            Random.nextInt(1000).toString().padStart(3, '0')

    private fun setUpTableIndexes(tableName: String, indexedColumns: List<String>) {
        for (col in indexedColumns) {
            val rnd = RandomStringUtils.insecure().nextAlphanumeric(3)
            val sql = "CREATE INDEX ${quote("idx_${tableName}_$rnd")} ON ${quote(tableName)} (${quote(col)})"
            log.debug("Creating index, SQL: $sql")
            hsqlDbHelper.executeSql(sql, "Failed to create index: ")
        }
    }

    companion object { private val log = logger() }
}
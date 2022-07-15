package cz.dynawest.csvcruncher.util

import cz.dynawest.csvcruncher.CsvCruncherException
import cz.dynawest.csvcruncher.HsqlDbHelper
import cz.dynawest.csvcruncher.HsqlDbHelper.Companion.quote
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.sql.SQLException

class HsqlDbTableCreator(val hsqlDbHelper: HsqlDbHelper) {

    @Throws(SQLException::class)
    fun createTableFromInputFile(tableName: String, csvFileToBind: File, columnNames: List<String>, ignoreFirst: Boolean, overwrite: Boolean) {
        createTableAndBindCsv(tableName, csvFileToBind, columnNames, ignoreFirst, "", true, overwrite)
    }

    @Suppress("SameParameterValue")
    @Throws(SQLException::class)
    private fun createTableAndBindCsv(tableName: String, csvFileToBind: File, columnsNames: List<String>, ignoreFirst: Boolean, counterColumnDdl: String, isInputTable: Boolean, overwrite: Boolean) {
        val columnsNamesAndTypes = Utils.listToMapKeysWithNullValues(columnsNames)
        createTableAndBindCsv(tableName, csvFileToBind, columnsNamesAndTypes, ignoreFirst, counterColumnDdl, isInputTable, overwrite)

        // Try to convert columns types to numbers, where applicable.
        if (isInputTable) {
            SqlTypeReducer(hsqlDbHelper)
                .optimizeTableColumnsType(tableName, columnsNames)
        }
    }

    /**
     * Creates the input or output table, with the right column names, and binds the file.
     * For output tables, the file is optionally overwritten if exists.
     * A header with columns names is added to the output table.
     * Input tables columns are optimized after binding the file by attempting to reduce the column type.
     * (The output table has to be optimized later.)
     */
    @Throws(SQLException::class)
    fun createTableAndBindCsv(tableName: String, csvFileToBind: File, columnsNamesAndTypes: Map<String, String?>, ignoreFirst: Boolean, counterColumnDdl: String?, isInputTable: Boolean, overwrite: Boolean) {
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
                if (true || overwrite) // TODO: Obey --overwrite.
                    csvFileToBind.delete() else throw IllegalArgumentException("The output file already exists. Use --overwrite or delete: $csvFileToBind")
            } else {
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
        for (columnDef in columnsNamesAndTypes.entries) {
            sbCsvHeader.append(columnDef.key).append(", ")

            val columnQuotedName = quote(columnDef.key)

            var columnType = columnDef.value
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


        // Bind the table to the CSV file.
        var csvPath = csvFileToBind.path
        csvPath = Utils.escapeSql(csvPath)
        val quoteCharacter = if (csvUsesSingleQuote) "\\quote" else "\""
        val ignoreFirstFlag = if (ignoreFirst) "ignore_first=true;" else ""
        val csvSettings = "encoding=UTF-8;cache_rows=50000;cache_size=10240000;" + ignoreFirstFlag + "fs=,;qc=" + quoteCharacter
        val DESC = if (readOnly) "DESC" else "" // Not a mistake, HSQLDB really has "DESC" here for read only.
        var sql = "SET TABLE ${quote(tableName)} SOURCE '$csvPath;$csvSettings' $DESC"
        log.debug("CSV import SQL: $sql")
        hsqlDbHelper.executeSql(sql, "Failed to import CSV: ")

        // SET TABLE <table name> SOURCE HEADER
        if (!isInputTable) {
            sql = "SET TABLE ${quote(tableName)} SOURCE HEADER '$sbCsvHeader'"
            log.debug("CSV source header SQL: $sql")
            hsqlDbHelper.executeSql(sql, "Failed to set CSV header: ")
        }
    }

    companion object { private val log = logger() }
}
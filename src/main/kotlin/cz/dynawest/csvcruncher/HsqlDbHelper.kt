package cz.dynawest.csvcruncher

import cz.dynawest.csvcruncher.util.DbUtils.analyzeWhatWasNotFound
import cz.dynawest.csvcruncher.util.DbUtils.getResultSetColumnNamesAndTypes
import cz.dynawest.csvcruncher.util.DbUtils.testDumpSelect
import cz.dynawest.csvcruncher.util.Utils.escapeSql
import lombok.extern.slf4j.Slf4j
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.sql.*

@Slf4j
@Suppress("NAME_SHADOWING")
class HsqlDbHelper(private val jdbcConn: Connection?) {

    private val log = org.slf4j.LoggerFactory.getLogger(HsqlDbHelper::class.java)
    
    @Throws(SQLException::class)
    fun createTableForInputFile(tableName: String, csvFileToBind: File, colNames: List<String>, ignoreFirst: Boolean, overwrite: Boolean) {
        createTableAndBindCsv(tableName, csvFileToBind, colNames, ignoreFirst, "", true, overwrite)
    }

    @Throws(SQLException::class)
    private fun createTableAndBindCsv(tableName: String, csvFileToBind: File, columnsNames: List<String>, ignoreFirst: Boolean, counterColumnDdl: String, isInputTable: Boolean, overwrite: Boolean) {
        val columnsDef = listToMapKeysWithNullValues(columnsNames)
        createTableAndBindCsv(tableName, csvFileToBind, columnsDef, ignoreFirst, counterColumnDdl, isInputTable, overwrite)

        // Try to convert columns types to numbers, where applicable.
        if (isInputTable) {
            optimizeTableCoumnsType(tableName, columnsNames)
        }
    }

    /**
     * Creates the input or output table, with the right column names, and binds the file.<br></br>
     * For output tables, the file is optionally overwritten if exists.<br></br>
     * A header with columns names is added to the output table.<br></br>
     * Input tables columns are optimized after binding the file by attempting to reduce the column type.
     * (The output table has to be optimized later.)<br></br>
     */
    @Throws(SQLException::class)
    fun createTableAndBindCsv(tableName: String?, csvFileToBind: File, columnsNamesAndTypes: Map<String, String?>, ignoreFirst: Boolean, counterColumnDdl: String?, isInputTable: Boolean, overwrite: Boolean) {
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
        val sbSql = StringBuilder("CREATE TEXT TABLE ").append(tableName).append(" ( ")

        // The counter column, if any.
        sbSql.append(counterColumnDdl)

        // Columns
        for (columnDef in columnsNamesAndTypes.entries) {
            val columnName = escapeSql(columnDef.key)
            var columnType = columnDef.value
            columnType =
                if (columnType == null || "VARCHAR" == columnType.toUpperCase())
                    "VARCHAR(" + MAX_STRING_COLUMN_LENGTH + ")"
                else escapeSql(columnType)
            sbCsvHeader.append(columnName).append(", ")
            sbSql.append(columnName).append(" ").append(columnType).append(", ")
        }
        sbCsvHeader.delete(sbCsvHeader.length - 2, sbCsvHeader.length)
        sbSql.delete(sbSql.length - 2, sbSql.length)
        sbSql.append(" )")
        log.debug("Table DDL SQL: $sbSql")
        executeDbCommand(sbSql.toString(), "Failed to CREATE TEXT TABLE: ")


        // Bind the table to the CSV file.
        var csvPath = csvFileToBind.path
        csvPath = escapeSql(csvPath)
        val quoteCharacter = if (csvUsesSingleQuote) "\\quote" else "\""
        val ignoreFirstFlag = if (ignoreFirst) "ignore_first=true;" else ""
        val csvSettings = "encoding=UTF-8;cache_rows=50000;cache_size=10240000;" + ignoreFirstFlag + "fs=,;qc=" + quoteCharacter
        val DESC = if (readOnly) "DESC" else "" // Not a mistake, HSQLDB really has "DESC" here for read only.
        var sql = String.format("SET TABLE %s SOURCE '%s;%s' %s", tableName, csvPath, csvSettings, DESC)
        log.debug("CSV import SQL: $sql")
        executeDbCommand(sql, "Failed to import CSV: ")

        // SET TABLE <table name> SOURCE HEADER
        if (!isInputTable) {
            sql = String.format("SET TABLE %s SOURCE HEADER '%s'", tableName, sbCsvHeader.toString())
            log.debug("CSV source header SQL: $sql")
            executeDbCommand(sql, "Failed to set CSV header: ")
        }
    }

    /**
     * Execute a SQL which does not expect a ResultSet,
     * and help the user with the common errors by parsing the message
     * and printing out some helpful info in a wrapping exception.
     *
     * @return the number of affected rows.
     */
    fun executeDbCommand(sql: String, errorMsg: String): Int {
        var errorMsg = errorMsg
        try {
            jdbcConn!!.createStatement().use { stmt -> return stmt.executeUpdate(sql) }
        } catch (ex: Exception) {
            var addToMsg = ""
            if (true || (ex.message?.contains("for cast") ?: false)) {
                // List column names with types.
                addToMsg = """
  Tables and column types:
${this.formatListOfAvailableTables(true)}"""
            }
            if (ex.message!!.contains("cannot be converted to target type")) {
                errorMsg = StringUtils.defaultString(errorMsg) + " Looks like the data in the input files do not match."
            }
            if (StringUtils.isBlank(errorMsg)) errorMsg = "Looks like there was a data type mismatch. Check the output table column types and your SQL."
            throw CsvCruncherException("""$errorMsg
  SQL: $sql
  DB error: ${ex.javaClass.simpleName} ${ex.message}$addToMsg""".trimMargin()
            )
        }
    }

    /**
     * Prepares a list of tables in the given JDBC connections, in the PUBLIC schema.
     */
    fun formatListOfAvailableTables(withColumns: Boolean): String {
        val schema = "'PUBLIC'"
        val sb = StringBuilder()
        val sqlTablesMetadata = "SELECT table_name AS t, c.column_name AS c, c.data_type AS ct" +
                " FROM INFORMATION_SCHEMA.TABLES AS t " +
                " NATURAL JOIN INFORMATION_SCHEMA.COLUMNS AS c " +
                " WHERE t.table_schema = " + schema
        try {
            jdbcConn!!.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY).use { st ->
                val rs = st.executeQuery(sqlTablesMetadata)
                tables@ while (rs.next()) {
                    val tableName = rs.getString("T")
                    sb.append(" * ").append(tableName).append('\n')
                    while (StringUtils.equals(tableName, rs.getString("T"))) {
                        if (withColumns) sb.append("    - ")
                                .append(StringUtils.rightPad(rs.getString("C"), 28))
                                .append(" ")
                                .append(rs.getString("CT"))
                                .append('\n')
                        if (!rs.next()) break@tables
                    }
                    rs.previous()
                }
                return if (sb.length == 0) "    (No tables)" else sb.toString()
            }
        } catch (ex: SQLException) {
            val msg = "Failed listing tables: " + ex.message
            log.error(msg)
            return msg
        }
    }

    /**
     * Analyzes the exception against the given DB connection and rethrows an exception with a message containing the available objects as a hint.
     */
    fun throwHintForObjectNotFound(ex: SQLSyntaxErrorException): CsvCruncherException {
        val notFoundIsColumn = analyzeWhatWasNotFound(ex.message!!)
        val tableNames = formatListOfAvailableTables(notFoundIsColumn)
        val hintMsg = if (notFoundIsColumn) """
  Looks like you are referring to a column that is not present in the table(s).
  Check the header (first line) in the CSV.
  Here are the tables and columns are actually available:
""" else """
  Looks like you are referring to a table that was not created.
  This could mean that you have a typo in the input file name,
  or maybe you use --combineInputs but try to use the original inputs.
  These tables are actually available:
"""
        return CsvCruncherException(
                """$hintMsg$tableNames
Message from the database:
  ${ex.message}""", ex)
    }

    /**
     * Get the columns info: Perform the SQL, LIMIT 1.
     */
    @Throws(SQLException::class)
    fun extractColumnsInfoFrom1LineSelect(sql: String): Map<String, String> {
        val statement: PreparedStatement
        statement = try {
            jdbcConn!!.prepareStatement("$sql LIMIT 1")
        } catch (ex: SQLSyntaxErrorException) {
            if (ex.message!!.contains("object not found:")) {
                throw throwHintForObjectNotFound(ex)
            }
            throw CsvCruncherException("""
    Seems your SQL contains errors:
    ${ex.message}
    """.trimIndent(), ex)
        } catch (ex: SQLException) {
            throw CsvCruncherException("""
    Failed executing the SQL:
    ${ex.message}
    """.trimIndent(), ex)
        }
        val rs = statement.executeQuery()

        // Column names
        return getResultSetColumnNamesAndTypes(rs)
    }

    /**
     * Detaches or re-attaches HSQLDB TEXT table.
     * @param drop     Drop the table after detaching.
     */
    @Throws(SQLException::class)
    fun detachTable(tableName: String?, drop: Boolean) {
        log.debug(String.format("Detaching%s table: %s", if (drop) " and dropping" else "", tableName))
        var sql = "SET TABLE " + escapeSql(tableName!!) + " SOURCE OFF"
        executeDbCommand(sql, "Failed to detach/attach the table: ")
        if (drop) {
            sql = "DROP TABLE " + escapeSql(tableName)
            executeDbCommand(sql, "Failed to DROP TABLE: ")
        }
    }

    @Throws(SQLException::class)
    fun attachTable(tableName: String) {
        log.debug("Ataching table: $tableName")
        val sql = "SET TABLE " + escapeSql(tableName) + " SOURCE ON"
        executeDbCommand(sql, "Failed to attach the table: ")
    }

    /**
     * This must be called when all data are already in the table!
     * Try to convert columns to best fitting types.
     * This speeds up further SQL operations. It also allows proper types for JSON (or other type-aware formats).
     *
     * "HyperSQL allows changing the type if all the existing values can be cast
     * into the new type without string truncation or loss of significant digits."
     */
    @Throws(SQLException::class)
    fun optimizeTableCoumnsType(tableName: String, colNames: List<String?>) {
        val columnsFitIntoType: MutableMap<String?, String> = LinkedHashMap()

        // TODO: This doesn't work because: Operation is not allowed on text table with data in statement.
        // See https://stackoverflow.com/questions/52647738/hsqldb-hypersql-changing-column-type-in-a-text-table
        // Maybe I need to duplicate the TEXT table into a native table first?
        for (colName in colNames) {
            var typeUsed: String?

            // Note: Tried also "CHAR", but seems that HSQL does some auto casting and the values wouldn't fit. Need to investigate.
            // Note: Tried also "VARCHAR(255)", but size limits is not handled below.
            for (sqlType in arrayOf("TIMESTAMP", "UUID", "BIGINT", "INTEGER", "SMALLINT", "BOOLEAN")) {
                // Try CAST( AS ...)
                val sqlCol = String.format("SELECT CAST(%s AS %s) FROM %s", colName, sqlType, tableName)
                //String sqlCol = String.format("SELECT 1 + \"%s\" FROM %s", colName, tableName);
                log.trace("Column change attempt SQL: $sqlCol")
                try {
                    jdbcConn!!.createStatement().use { st ->
                        st.execute(sqlCol)
                        log.trace(String.format("Column %s.%s fits to %s", tableName, colName, sqlType.also { typeUsed = it }))
                        columnsFitIntoType.put(colName, sqlType)
                    }
                } catch (ex: SQLException) {
                    // LOG.trace(String.format("Column %s.%s values don't fit to %s.\n  %s", tableName, colName, sqlType, ex.getMessage()));
                }
            }
        }
        detachTable(tableName, false)

        // ALTER COLUMNs
        for ((colName, sqlType) in columnsFitIntoType) {
            val sqlAlter = String.format("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s", tableName, colName, sqlType)
            val sqlCheck = String.format("SELECT data_type FROM information_schema.columns WHERE LOWER(table_name) = LOWER('%s') AND LOWER(column_name) = LOWER('%s')", tableName, colName)
            log.trace("Changing the column {} to {}", colName, sqlType)
            try {
                jdbcConn!!.createStatement().use { st ->
                    st.execute(sqlAlter)
                    log.debug(String.format("Column %s.%-20s converted to %-14s %s", tableName, colName, sqlType, sqlAlter))
                    log.trace("Checking col type: $sqlCheck")
                    val columnTypeRes = st.executeQuery(sqlCheck)
                    if (!columnTypeRes.next()) {
                        log.error("Column not found?? {}.{}", tableName, colName)
                        testDumpSelect("SELECT table_name, column_name FROM information_schema.columns WHERE LOWER(table_name) = LOWER('" + tableName.toUpperCase() + "')", jdbcConn)
                        throw ColumnNotFoundException(tableName, colName)
                    }
                    val newType = columnTypeRes.getString("data_type")
                    if (newType != sqlType) {
                        log.error(String.format("Column %s.%s did not really change the type to %s, stayed %s.", tableName, colName, sqlType, newType))
                    }
                }
            } catch (ex: SQLException) {
                log.error(String.format("Error changing type of column %s.%s to %s.\n  %s", tableName, colName, sqlType, ex.message))
            }
            catch (ex: ColumnNotFoundException) {
                continue
            }
        }
        attachTable(tableName)
    }

    class ColumnNotFoundException(tableName: String, colName: String?) : Exception("Column not found: $tableName.$colName")

    fun detachTables(tableNames: Set<String?>, msgOnError: String) {
        for (tableName in tableNames) {
            try {
                detachTable(tableName, true)
            } catch (ex: Exception) {
                log.error(msgOnError + ex.message)
            }
        }
    }

    companion object {
        private val log = org.slf4j.LoggerFactory.getLogger(FileUtils::class.java)
        const val MAX_STRING_COLUMN_LENGTH = 4092

        /**
         * Returns a map with keys from the given list, and null values. Doesn't deal with duplicate keys.
         */
        private fun listToMapKeysWithNullValues(keys: List<String>): Map<String, String?> {
            val result = LinkedHashMap<String, String?>()
            for (columnsName in keys) {
                result[columnsName] = null
            }
            return result
        }

        fun normalizeFileNameForTableName(fileName: File): String {
            return fileName.name.replaceFirst(".csv$".toRegex(), "").replace("[^a-zA-Z0-9_]".toRegex(), "_")
        }
    }
}
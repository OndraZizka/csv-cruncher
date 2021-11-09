package cz.dynawest.csvcruncher

import cz.dynawest.csvcruncher.util.DbUtils.getResultSetColumnNamesAndTypes
import cz.dynawest.csvcruncher.util.DbUtils.testDumpSelect
import cz.dynawest.csvcruncher.util.HsqldbErrorHandling.throwHintForObjectNotFound
import cz.dynawest.csvcruncher.util.Utils.escapeSql
import cz.dynawest.csvcruncher.util.logger
import org.apache.commons.lang3.StringUtils
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLSyntaxErrorException

@Suppress("NAME_SHADOWING")
class HsqlDbHelper(private val jdbcConn: Connection) {


    /**
     * Execute a SQL which does not expect a ResultSet,
     * and help the user with the common errors by parsing the message
     * and printing out some helpful info in a wrapping exception.
     *
     * @return the number of affected rows.
     */
    fun executeSql(sql: String, errorMsg: String): Int {
        if (sql.isBlank()) throw CsvCruncherException("$errorMsg: SQL is an empty string.")

        var errorMsg = errorMsg
        try {
            log.debug("Executing SQL: $sql")
            jdbcConn.createStatement().use { stmt -> return stmt.executeUpdate(sql) }
        } catch (ex: Exception) {
            var addToMsg = ""
            if (true || (ex.message?.contains("for cast") ?: false)) {
                // List column names with types.
                addToMsg = """
                    |  Tables and column types:
                    |${this.formatListOfAvailableTables(true)}""".trimMargin()
            }
            if (ex.message!!.contains("cannot be converted to target type")) {
                errorMsg = "$errorMsg Looks like the data in the input files do not match."
            }
            if (errorMsg.isBlank()) errorMsg = "Looks like there was a data type mismatch. Check the output table column types and your SQL."
            throw CsvCruncherException("""$errorMsg
                |  SQL: $sql
                |  DB error: ${ex.javaClass.simpleName} ${ex.message}$addToMsg""".trimMargin()
            )
        }
    }

    /**
     * Prepares a list of tables in the given JDBC connections, in the PUBLIC schema.
     */
    fun formatListOfAvailableTables(withColumns: Boolean): String {
        val schema = "PUBLIC"
        val sqlTablesMetadata = "SELECT table_name AS t, c.column_name AS c, c.data_type AS ct FROM INFORMATION_SCHEMA.TABLES AS t" +
            " NATURAL JOIN INFORMATION_SCHEMA.COLUMNS AS c  WHERE t.table_schema = '$schema'"

        try {
            jdbcConn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY).use { st ->
                val rs = st.executeQuery(sqlTablesMetadata)
                val sb = StringBuilder()
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
     * Get the columns info: Perform the SQL, LIMIT 1.
     */
    @Throws(SQLException::class)
    fun extractColumnsInfoFrom1LineSelect(sql: String): Map<String, String> {

        val sql = setOrReplaceLimit(sql, "LIMIT 1")

        val statement: PreparedStatement
        statement = try {
            jdbcConn.prepareStatement("$sql")
        } catch (ex: SQLSyntaxErrorException) {
            if (ex.message!!.contains("object not found:")) {
                throw throwHintForObjectNotFound(ex, this)
            }
            throw CsvCruncherException("""
                |    Seems your SQL contains errors:
                |    ${ex.message}
                |    $sql
                |    """.trimMargin(), ex)
        } catch (ex: SQLException) {
            throw CsvCruncherException("""
                |    Failed executing the SQL:
                |    ${ex.message}
                |    $sql
                |    """.trimMargin(), ex)
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
        log.debug("Detaching${if (drop) " and dropping" else ""} table: $tableName")
        var sql = "SET TABLE " + escapeSql(tableName!!) + " SOURCE OFF"
        executeSql(sql, "Failed to detach/attach the table: ")
        if (drop) {
            sql = "DROP TABLE " + escapeSql(tableName)
            executeSql(sql, "Failed to DROP TABLE: ")
        }
    }

    @Throws(SQLException::class)
    fun attachTable(tableName: String) {
        log.debug("Ataching table: $tableName")
        val sql = "SET TABLE " + escapeSql(tableName) + " SOURCE ON"
        executeSql(sql, "Failed to attach the table: ")
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

            // Note: Tried also "CHAR", but seems that HSQL does some auto casting and the values wouldn't fit. Need to investigate.
            // Note: Tried also "VARCHAR(255)", but size limits is not handled below.
            for (sqlType in arrayOf("TIMESTAMP", "UUID", "BIGINT", "INTEGER", "SMALLINT", "BOOLEAN")) {
                // Try CAST( AS ...)
                val sqlCol = "SELECT CAST($colName AS $sqlType) FROM $tableName"
                //String sqlCol = String.format("SELECT 1 + \"%s\" FROM %s", colName, tableName);
                log.trace("Column change attempt SQL: $sqlCol")
                try {
                    jdbcConn.createStatement().use { st ->
                        st.execute(sqlCol)
                        log.trace("Column $tableName.$colName fits to $sqlType")
                        columnsFitIntoType.put(colName, sqlType)
                    }
                } catch (ex: SQLException) {
                    // log.trace(String.format("Column %s.%s values don't fit to %s.\n  %s", tableName, colName, sqlType, ex.getMessage()));
                }
            }
        }
        detachTable(tableName, false)

        // ALTER COLUMNs
        for ((colName, sqlType) in columnsFitIntoType) {
            val sqlAlter = "ALTER TABLE $tableName ALTER COLUMN $colName SET DATA TYPE $sqlType"
            val sqlCheck =
                "SELECT data_type FROM information_schema.columns WHERE LOWER(table_name) = LOWER('$tableName') AND LOWER(column_name) = LOWER('$colName')"
            log.trace("Changing the column {} to {}", colName, sqlType)
            try {
                jdbcConn.createStatement().use { st ->
                    st.execute(sqlAlter)
                    log.debug(String.format("Column %s.%-20s converted to %-14s %s", tableName, colName, sqlType, sqlAlter))
                    log.trace("Checking col type: $sqlCheck")
                    val columnTypeRes = st.executeQuery(sqlCheck)
                    if (!columnTypeRes.next()) {
                        log.error("Column not found?? {}.{}", tableName, colName)
                        testDumpSelect("SELECT table_name, column_name FROM information_schema.columns WHERE LOWER(table_name) = LOWER('" + tableName.uppercase() + "')", jdbcConn)
                        throw ColumnNotFoundException(tableName, colName)
                    }
                    val newType = columnTypeRes.getString("data_type")
                    if (newType != sqlType) {
                        log.error("Column $tableName.$colName did not really change the type to $sqlType, stayed $newType.")
                    }
                }
            } catch (ex: SQLException) {
                log.error("Error changing type of column $tableName.$colName to $sqlType.\n  ${ex.message}")
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

    fun queryAllColumnNames(): List<String> {
        val schema = "PUBLIC"
        val sqlTablesMetadata = """SELECT c.column_name AS "colName" FROM INFORMATION_SCHEMA.TABLES AS t
             NATURAL JOIN INFORMATION_SCHEMA.COLUMNS AS c  WHERE t.table_schema = '$schema'"""

        try {
            val columnNames = mutableListOf<String>()
            jdbcConn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY).use { st ->
                val resultSet = st.executeQuery(sqlTablesMetadata)
                while (resultSet.next()) {
                    columnNames.add(resultSet.getString("colName"))
                }
            }
            return columnNames
        } catch (ex: SQLException) {
            throw CsvCruncherException("Couldn't list all columns: ${ex.message}", ex)
        }
    }

    fun executeSqlScript(path: Path, errorMsg: String) {
        if (!path.toFile().exists()) throw CsvCruncherException("$errorMsg: Does not exist: $path")
        if (!path.toFile().isFile()) throw CsvCruncherException("$errorMsg: Is not a file: $path")

        for (line in path.toFile().readLines().asSequence().map { it.trim().removeSuffix(";") }) {
            if (line.isBlank()) continue
            if (line.startsWith("--")) continue
            executeSql(line, errorMsg)
        }
    }

    companion object {
        private val log = logger()
        const val MAX_STRING_COLUMN_LENGTH = 4092

        /**
         * Returns a map with keys from the given list, and null values. Doesn't deal with duplicate keys.
         */
        fun listToMapKeysWithNullValues(keys: List<String>): Map<String, String?> {
            val result = LinkedHashMap<String, String?>()
            for (columnsName in keys) {
                result[columnsName] = null
            }
            return result
        }

        fun normalizeFileNameForTableName(fileName: File): String {
            return fileName.name.replaceFirst(".csv$".toRegex(), "").replace("[^a-zA-Z0-9_]".toRegex(), "_")
        }

        fun quote(identifier: String) = "\"${identifier.replace('"', '\'')}\""

        fun setOrReplaceLimit(sql: String, newLimitClause: String) = sql.replace("\\s*\\bLIMIT\\s+\\d+(\\s+OFFSET\\s+\\d+)?\\s*\$".toRegex(), "") + " $newLimitClause"

        /**
         * Because of HSQLDB's rules of column names normalization, the column names need to be quoted in queries (or be all uppercased).
         * To relieve the user from quoting everything, this method does it.
         * Although, the current impl is potentially brittle.
         */
        fun quoteColumnNamesInQuery(sqlQuery: String, columnNames: List<String>): String {
            var sqlQuery = sqlQuery
            val columnNames = columnNames.sortedByDescending { it.length }
            val substitutes = mutableMapOf<String, String>()

            for (name in columnNames.withIndex()) {
                val substitute = "{.${name.index}}"
                substitutes.put(substitute, "\"${name.value}\"")
                val escapedName = Regex.escape(name.value)
                sqlQuery = sqlQuery.replace("""(?i)\b(?<!")$escapedName(?!")\b""".toRegex(), Regex.escapeReplacement(substitute))
            }
            for (i in columnNames.indices) {
                val substitute = "{.${i}}"
                sqlQuery = sqlQuery.replace(substitute, substitutes.get(substitute)!!)
            }

            return sqlQuery
        }
    }
}
package cz.dynawest.csvcruncher

import cz.dynawest.csvcruncher.util.DbUtils.getResultSetColumnLabelsAndTypes
import cz.dynawest.csvcruncher.util.HsqldbErrorHandling.throwHintForObjectNotFound
import cz.dynawest.csvcruncher.util.logger
import org.apache.commons.lang3.StringUtils
import java.nio.file.Path
import java.sql.*

@Suppress("NAME_SHADOWING")
class HsqlDbHelper(val jdbcConn: Connection) {

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
            log.debug("    Executing SQL: $sql")
            jdbcConn.createStatement().use { stmt -> return stmt.executeUpdate(sql) }
        }
        // TBD: Handle SQLSyntaxErrorException specifically
        catch (ex: Exception) {
            var addToMsg = ""
            if (true || (ex.message?.contains("for cast") ?: false)) {
                // List column names with types.
                addToMsg = """
                    |
                    |  Tables and column types:
                    |${this.formatListOfAvailableTables(true)}""".trimMargin()
            }
            if (ex.message!!.contains("cannot be converted to target type")) {
                errorMsg = "$errorMsg Looks like the data in the input files do not match."
            }
            if (errorMsg.isBlank()) errorMsg = "Looks like there was a data type mismatch. Check the output table column types and your SQL."
            throw CsvCruncherException("""$errorMsg
                |  DB error: ${ex.javaClass.simpleName} ${ex.message}$addToMsg
                |  SQL: $sql
                """.trimMargin()
            )
        }
    }

    /**
     * Prepares a list of tables in the given JDBC connections, in the PUBLIC schema.
     */
    fun formatListOfAvailableTables(withColumns: Boolean): String {
        val schema = "PUBLIC"
        val sqlTablesMetadata =
            "SELECT t.table_name AS t, c.column_name AS c, c.data_type AS ct " +
            " FROM INFORMATION_SCHEMA.TABLES AS t" +
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
                return if (sb.isEmpty()) "    (No tables)" else sb.toString()
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
            jdbcConn.prepareStatement(sql)
        }
        catch (ex: SQLSyntaxErrorException) {
            if (ex.message!!.contains("unexpected token:")) {
                throw SqlSyntaxCruncherException("""
                |    The SQL contains syntax error:
                |    ${ex.message}
                |    $sql
                |    This may be your SQL error or caused by alteration by CsvCruncher. 
                |    See https://github.com/OndraZizka/csv-cruncher/issues for known bugs.
                |    """.trimMargin())
            }
            if (ex.message!!.contains("object not found:")) {
                throw throwHintForObjectNotFound(ex, this, sql)
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
        return getResultSetColumnLabelsAndTypes(rs)
    }


    /**
     * Detaches or re-attaches HSQLDB TEXT table.
     * @param drop     Drop the table after detaching.
     */
    @Throws(SQLException::class)
    fun detachTable(tableName: String, drop: Boolean) {
        log.debug("Detaching${if (drop) " and dropping" else ""} table: $tableName")
        var sql = "SET TABLE ${quote(tableName)} SOURCE OFF"
        executeSql(sql, "Failed to detach/attach the table: ")
        if (drop) {
            sql = "DROP TABLE ${quote(tableName)}"
            executeSql(sql, "Failed to DROP TABLE: ")
        }
    }

    @Throws(SQLException::class)
    fun attachTable(tableName: String) {
        log.debug("Attaching table: $tableName")
        val sql = "SET TABLE ${quote(tableName)} SOURCE ON"
        executeSql(sql, "Failed to attach the table: ")
    }



    class ColumnNotFoundException(tableName: String, colName: String?) : Exception("Column not found: $tableName.$colName")

    fun detachTables(tableNames: Set<String>, msgOnError: String) {
        for (tableName in tableNames) {
            try {
                detachTable(tableName, true)
            } catch (ex: Exception) {
                log.warn(msgOnError + ex.message)
            }
        }
    }

    fun queryAllTableAndColumnNames(includeTables: Boolean = false, includeColumns: Boolean = true): Set<String> {
        val schema = "PUBLIC"
        val sqlTablesMetadata =
            """SELECT t.table_name AS "tableName", c.column_name AS "colName" 
                 FROM INFORMATION_SCHEMA.TABLES AS t
                 NATURAL JOIN INFORMATION_SCHEMA.COLUMNS AS c  
                 WHERE t.table_schema = '$schema'
             """

        try {
            val identifiers = mutableSetOf<String>()
            jdbcConn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY).use { st ->
                val resultSet = st.executeQuery(sqlTablesMetadata)
                while (resultSet.next()) {
                    if (includeTables)  identifiers.add(resultSet.getString("tableName"))
                    if (includeColumns) identifiers.add(resultSet.getString("colName"))
                }
            }
            return identifiers
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

    fun quoteColumnAndTableNamesInQuery(sqlQuery: String): String {
        val identifiers = mutableListOf<String>()
        identifiers.addAll(queryAllTableAndColumnNames(includeTables = true, includeColumns = true))
        return quoteIdentifiersInQuery(sqlQuery, identifiers)
    }


    companion object {
        private val log = logger()
        const val MAX_STRING_COLUMN_LENGTH = 4092

        fun normalizeTableName(proposedName: String): String {
            return proposedName.replace("[^a-zA-Z0-9_]".toRegex(), "_")
        }

        fun quote(identifier: String) = "\"${identifier.replace('"', '\'')}\""

        fun setOrReplaceLimit(sql: String, newLimitClause: String) = sql.replace("\\s*\\bLIMIT\\s+\\d+(\\s+OFFSET\\s+\\d+)?\\s*\$".toRegex(), "") + " $newLimitClause"

        /**
         * Because of HSQLDB's rules of column names normalization, the column names need to be quoted in queries (or be all uppercased).
         * To relieve the user from quoting everything, this method does it.
         * Although, the current impl is potentially brittle.
         */
        fun quoteIdentifiersInQuery(sqlQuery: String, identifiers: List<String>): String {
            var sqlQuery = sqlQuery
            val columnNames = identifiers.sortedByDescending { it.length }
            val substitutes = mutableMapOf<String, String>()

            for (name in columnNames.withIndex()) {
                val substitute = "{.${name.index}}"
                substitutes[substitute] = "\"${name.value}\""
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

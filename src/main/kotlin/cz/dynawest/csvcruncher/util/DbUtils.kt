package cz.dynawest.csvcruncher.util

import org.apache.commons.lang3.StringUtils
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.util.*

object DbUtils {
    @Throws(SQLException::class)
    fun getResultSetColumnNames(rs: ResultSet): List<String> {
        val colNames_ = arrayOfNulls<String>(rs.metaData.columnCount)
        for (colIndex in colNames_.indices) {
            colNames_[colIndex] = rs.metaData.getColumnName(colIndex + 1).lowercase()
        }
        return Arrays.asList(*colNames_).map { it!! }
    }

    @JvmStatic
    @Throws(SQLException::class)
    fun getResultSetColumnNamesAndTypes(rs: ResultSet): Map<String, String> {
        val columnCount = rs.metaData.columnCount
        val columns: MutableMap<String, String> = LinkedHashMap(columnCount)
        for (colIndex in 0 until columnCount) {
            columns[rs.metaData.getColumnName(colIndex + 1)] = rs.metaData.getColumnTypeName(colIndex + 1)
        }
        return columns
    }

    /**
     * Tells apart whether the "object not found" was a column or a table.
     * Relies on HSQLDB's exception message, which looks like this:
     * USER LACKS PRIVILEGE OR OBJECT NOT FOUND: JOBNAME IN STATEMENT [SELECT JOBNAME, FROM
     *
     * user lacks privilege or object not found: JOBNAME in statement [SELECT jobName, ... FROM ...]
     *
     * @return true if column, false if table (or something else).
     */
    @JvmStatic
    fun analyzeWhatWasNotFound(message: String): Boolean {
        @Suppress("NAME_SHADOWING")
        var message = message
        var notFoundName = StringUtils.substringAfter(message, "object not found: ")
        notFoundName = StringUtils.substringBefore(notFoundName, " in statement [")
        message = message.uppercase().replace('\n', ' ')

        //String sqlRegex = "[^']*\\[SELECT .*" + notFoundName + ".*FROM.*";
        val sqlRegex = ".*SELECT.*$notFoundName.*FROM.*"
        //LOG.finer(String.format("\n\tNot found object: %s\n\tMsg: %s\n\tRegex: %s", notFoundName, message.toUpperCase(), sqlRegex));
        return message.uppercase().matches(sqlRegex.toRegex())
    }

    /**
     * Dump the content of a table. Debug code.
     */
    @JvmStatic
    @Throws(SQLException::class)
    @Suppress("NAME_SHADOWING")
    fun testDumpSelect(sql: String, jdbcConn: Connection) {
        var sql = sql
        sql = if (sql.startsWith("SELECT ")) sql else "SELECT * FROM $sql"
        val ps = jdbcConn.createStatement()
        val rs = ps.executeQuery(sql)
        val metaData = rs.metaData
        while (rs.next()) {
            println(" ------- ")
            for (i in 1..metaData.columnCount) {
                println(" " + metaData.getColumnLabel(i) + ": " + rs.getObject(i))
            }
        }
    }
}
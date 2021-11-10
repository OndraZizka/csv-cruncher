package cz.dynawest.csvcruncher.util

import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger
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
     * Dump the content of a table. Debug code.
     */
    @Throws(SQLException::class)
    @Suppress("NAME_SHADOWING")
    fun testDumpSelect(sql: String, jdbcConn: Connection, log: Logger) {
        var sql = sql
        sql = if (sql.startsWith("SELECT ")) sql else "SELECT * FROM $sql"
        val ps = jdbcConn.createStatement()
        val rs = ps.executeQuery(sql)
        testDumpResultSet(rs, log)
    }

    fun testDumpResultSet(rs: ResultSet, log: Logger) {
        val metaData = rs.metaData
        if (rs.isBeforeFirst) rs.next()
        do {
            log.warn((1..metaData.columnCount).map { i ->
                (metaData.getColumnLabel(i) + ": " + rs.getObject(i))
            }.joinToString(separator = ", "))
        }
        while (rs.next())
    }
}
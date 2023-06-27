package cz.dynawest.csvcruncher.util

import org.slf4j.Logger
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.util.*

object DbUtils {

    @JvmStatic
    @Throws(SQLException::class)
    fun getResultSetColumnLabelsAndTypes(rs: ResultSet): Map<String, String> {
        val columnCount = rs.metaData.columnCount
        val columns: MutableMap<String, String> = LinkedHashMap(columnCount)
        for (colIndex in 0 until columnCount) {
            columns[rs.metaData.getColumnLabel(colIndex + 1)] = rs.metaData.getColumnTypeName(colIndex + 1)
        }
        return columns
    }

    /**
     * Dump the content of a table. Debug code.
     */
    @Throws(SQLException::class)
    @Suppress("NAME_SHADOWING", "unused")
    fun testDumpSelect(sql: String, jdbcConn: Connection, log: Logger) {
        var sql = sql
        sql = if (sql.startsWith("SELECT ")) sql else "SELECT * FROM $sql"
        val ps = jdbcConn.createStatement()
        val rs = ps.executeQuery(sql)
        testDumpResultSet(rs, log)
    }

    @Suppress("MemberVisibilityCanBePrivate")
    fun testDumpResultSet(rs: ResultSet, log: Logger) {
        val metaData = rs.metaData
        if (rs.isBeforeFirst) rs.next()
        do {
            log.warn((1..metaData.columnCount).joinToString(separator = ", ") { i ->
                (metaData.getColumnLabel(i) + ": " + rs.getObject(i))
            })
        }
        while (rs.next())
    }
}
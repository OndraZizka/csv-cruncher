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
            for (i in 1..metaData.columnCount) {
                print(" " + metaData.getColumnLabel(i) + ": " + rs.getObject(i))
            }
            println()
        }
    }
}
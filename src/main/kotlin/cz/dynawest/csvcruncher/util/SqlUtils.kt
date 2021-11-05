package cz.dynawest.csvcruncher.util

import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.sql.Types

object SqlUtils {

    /**
     * This is for the case we use hand-made JSON marshalling.
     * Returns null if the column value was null, or if the returned type is not supported.
     */
    @Throws(SQLException::class)
    private fun formatSqlValueForJson(resultSet: ResultSet, colIndex: Int, colsAreNumbers: BooleanArray): String? {
        val metaData = resultSet.metaData
        val value: String
        value = when (metaData.getColumnType(colIndex)) {
            Types.VARCHAR, Types.CHAR, Types.CLOB -> resultSet.getString(colIndex)
            Types.TINYINT, Types.BIT -> "" + resultSet.getByte(colIndex)
            Types.SMALLINT -> "" + resultSet.getShort(colIndex)
            Types.INTEGER -> "" + resultSet.getInt(colIndex)
            Types.BIGINT -> "" + resultSet.getLong(colIndex)
            Types.BOOLEAN -> "" + resultSet.getBoolean(colIndex)
            Types.FLOAT -> "" + resultSet.getFloat(colIndex)
            Types.DOUBLE, Types.DECIMAL -> "" + resultSet.getDouble(colIndex)
            Types.NUMERIC -> "" + resultSet.getBigDecimal(colIndex)
            Types.DATE -> "" + resultSet.getDate(colIndex)
            Types.TIME -> "" + resultSet.getTime(colIndex)
            Types.TIMESTAMP -> ("" + resultSet.getTimestamp(colIndex)).replace(' ', 'T')
            else -> {
                log.error("Unsupported type of column " + metaData.getColumnLabel(colIndex) + ": " + metaData.getColumnTypeName(colIndex))
                return null
            }
        }
        return if (resultSet.wasNull()) null else value
    }

    
    @Throws(SQLException::class)
    private fun cacheWhichColumnsNeedJsonQuotes(metaData: ResultSetMetaData): BooleanArray {
        val colsAreNumbers = BooleanArray(metaData.columnCount + 1)

        for (colIndex in 1..metaData.columnCount) {
            colsAreNumbers[colIndex] = when (metaData.getColumnType(colIndex)) {
                Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.DECIMAL, Types.NUMERIC, Types.BIT, Types.ROWID, Types.DOUBLE, Types.FLOAT, Types.BOOLEAN -> true
                else -> false
            }
        }
        return colsAreNumbers
    }


    private val log = logger()
}
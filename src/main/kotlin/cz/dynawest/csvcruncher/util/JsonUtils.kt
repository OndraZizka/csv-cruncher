package cz.dynawest.csvcruncher.util

import cz.dynawest.csvcruncher.CsvCruncherException
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
import javax.json.Json
import javax.json.JsonObjectBuilder

object JsonUtils {

    /**
     * Writes the given resultset to a JSON file at given path, one entry per line, optionally as an JSON array.
     */
    @JvmStatic
    fun convertResultToJson(resultSet: ResultSet, destFile: Path, printAsArray: Boolean) {
        try {
            BufferedOutputStream(FileOutputStream(destFile.toFile())).use { outS ->
                OutputStreamWriter(outS, StandardCharsets.UTF_8).use { outW ->
                    val metaData = resultSet.metaData

                    // Cache which cols are numbers.

                    //boolean[] colsAreNumbers = cacheWhichColumnsNeedJsonQuotes(metaData);
                    if (printAsArray) outW.append("[\n")
                    while (resultSet.next()) {
                        // javax.json way
                        val builder = Json.createObjectBuilder()

                        // Columns
                        for (colIndex in 1..metaData.columnCount) {
                            addTheRightTypeToJavaxJsonBuilder(resultSet, colIndex, builder)
                        }
                        val jsonObject = builder.build()
                        val writer = Json.createWriter(outW)
                        writer.writeObject(jsonObject)

                        outW.append(if (printAsArray) ",\n" else "\n")
                    }
                    if (printAsArray) outW.append("]\n")
                }
            }
        } catch (ex: Exception) {
            throw CsvCruncherException("Failed browsing the final query results: " + ex.message, ex)
        }
    }

    /**
     * Used in case we use javax.json.JsonBuilder.
     * This also needs JsonProviderImpl.
     */
    @Throws(SQLException::class)
    private fun addTheRightTypeToJavaxJsonBuilder(resultSet: ResultSet, colIndex: Int, builder: JsonObjectBuilder) {
        val metaData = resultSet.metaData
        var columnLabel = metaData.getColumnLabel(colIndex)
        if (columnLabel.matches("[A-Z][A-Z_]*".toRegex())) columnLabel = columnLabel.lowercase()
        if (resultSet.getObject(colIndex) == null) {
            builder.addNull(columnLabel)
            return
        }
        when (metaData.getColumnType(colIndex)) {
            Types.VARCHAR, Types.CHAR, Types.CLOB -> builder.add(columnLabel, resultSet.getString(colIndex))
            Types.TINYINT, Types.BIT -> builder.add(columnLabel, resultSet.getByte(colIndex).toInt())
            Types.SMALLINT -> builder.add(columnLabel, resultSet.getShort(colIndex).toInt())
            Types.INTEGER -> builder.add(columnLabel, resultSet.getInt(colIndex))
            Types.BIGINT -> builder.add(columnLabel, resultSet.getLong(colIndex))
            Types.BOOLEAN -> builder.add(columnLabel, resultSet.getBoolean(colIndex))
            Types.FLOAT, Types.DOUBLE -> builder.add(columnLabel, resultSet.getDouble(colIndex))
            Types.DECIMAL, Types.NUMERIC -> builder.add(columnLabel, resultSet.getBigDecimal(colIndex))
            Types.DATE -> builder.add(columnLabel, "" + resultSet.getDate(colIndex))
            Types.TIME -> builder.add(columnLabel, "" + resultSet.getTime(colIndex))
            Types.TIMESTAMP -> builder.add(columnLabel, ("" + resultSet.getTimestamp(colIndex)).replace(' ', 'T'))
        }
        // This should be handled by getObject(), but just in case...
        if (resultSet.wasNull()) {
            builder.addNull(columnLabel)
        }
    }

}
package cz.dynawest.csvcruncher.util

import cz.dynawest.csvcruncher.HsqlDbHelper
import java.sql.SQLException

class SqlTypeReducer(val hsqlDbHelper: HsqlDbHelper) {
    /**
     * This must be called when all data are already in the table!
     * Try to convert columns to best fitting types.
     * This speeds up further SQL operations. It also allows proper types for JSON (or other type-aware formats).
     *
     * "HyperSQL allows changing the type if all the existing values can be cast
     * into the new type without string truncation or loss of significant digits."
     */
    @Throws(SQLException::class)
    fun optimizeTableColumnsType(tableName: String, colNames: List<String>) {
        val columnsFitIntoType: MutableMap<String, String> = LinkedHashMap()

        // TODO: This doesn't work because: Operation is not allowed on text table with data in statement.
        // See https://stackoverflow.com/questions/52647738/hsqldb-hypersql-changing-column-type-in-a-text-table
        // Maybe I need to duplicate the TEXT table into a native table first?
        val qt = HsqlDbHelper.quote(tableName)
        for (colName in colNames) {
            val qc = HsqlDbHelper.quote(colName)

            // Note: Tried also "CHAR", but seems that HSQL does some auto casting and the values wouldn't fit. Need to investigate.
            // Note: Tried also "VARCHAR(255)", but size limits is not handled below.
            for (sqlType in arrayOf("TIMESTAMP", "UUID", "DECIMAL(14,6)", "DECIMAL(10,3)", "DECIMAL(2,2)", "BIGINT", "INTEGER", "SMALLINT", "BOOLEAN")) {

                // Try CAST( AS ...)
                //val sqlCol = "SELECT CAST($qt.$qc AS $sqlType) = $qt.$qc AS same, $qt.$qc, CAST($qt.$qc AS $sqlType) AS casted, $qt.$qc - CAST($qt.$qc AS $sqlType) AS diff FROM $qt WHERE NOT CAST($qt.$qc AS $sqlType) = $qt.$qc"
                @Suppress("SqlResolve") val sqlCol =
                    """SELECT
                         '$sqlType' AS type, $qt.$qc, 
                         -- CAST($qt.$qc AS $sqlType) AS casted, 
                         -- CAST($qt.$qc AS $sqlType) = $qt.$qc AS equal, 
                         -- CAST(CAST($qt.$qc AS $sqlType) AS LONGVARCHAR) AS castedBack, 
                         CAST(CAST($qt.$qc AS $sqlType) AS LONGVARCHAR) = CAST($qt.$qc AS LONGVARCHAR) AS stringEqual,
                         STARTSWITH(CAST(CAST($qt.$qc AS $sqlType) AS LONGVARCHAR), $qt.$qc)  AS startsWith
                       FROM $qt
                       WHERE NOT STARTSWITH(LCASE(CAST(CAST($qt.$qc AS $sqlType) AS LONGVARCHAR)), LCASE($qt.$qc))
                    """
                log.trace("  Column change attempt SQL: $sqlCol")
                try {
                    hsqlDbHelper.jdbcConn.createStatement().use { st ->
                        val result = st.executeQuery(sqlCol)
                        val hasDifferences = result.next()
                        if (!hasDifferences) {
                            log.trace("  Column $tableName.$colName +++FITS+++ to $sqlType")
                            columnsFitIntoType.put(colName, sqlType)
                        }
                        else {
                            log.trace("Column $tableName.$colName ---DOES NOT FIT--- to $sqlType. The casted value looses information.");
                        }
                        //testDumpResultSet(result, log)
                    }
                } catch (ex: SQLException) {
                    log.trace("Column $tableName.$colName ---DOES NOT FIT--- to $sqlType. DB says: ${ex.message}");
                    // Possible messages:
                    // data exception: invalid character value for cast
                    // precision or scale out of range
                    // data exception: invalid datetime format
                }
            }
        }

        hsqlDbHelper.detachTable(tableName, false)

        // ALTER COLUMNs
        for ((colName, desiredSqlType) in columnsFitIntoType) {
            val sqlAlter = "ALTER TABLE $qt ALTER COLUMN ${HsqlDbHelper.quote(colName)} SET DATA TYPE $desiredSqlType"

            log.trace("Changing the column {} to {}", colName, desiredSqlType)

            try {
                hsqlDbHelper.jdbcConn.createStatement().use { st ->
                    st.execute(sqlAlter)
                    log.debug(String.format("Column %s.%-20s converted to %-14s %s", tableName, colName, desiredSqlType, sqlAlter))

                    val sqlCheck =
                        "SELECT data_type FROM information_schema.columns " +
                            "  WHERE LOWER(table_name) = LOWER('${Utils.escapeSql(tableName)}') " +
                            " AND LOWER(column_name) = LOWER('${Utils.escapeSql(colName)}')"
                    log.trace("Checking col type: $sqlCheck")
                    val columnTypeRes = st.executeQuery(sqlCheck)
                    if (!columnTypeRes.next()) {
                        log.error("Column not found?? {}.{}", tableName, colName)
                        //testDumpSelect("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE LOWER(table_name) = LOWER('${escapeSql(tableName)}')", jdbcConn, log)
                        throw HsqlDbHelper.ColumnNotFoundException(tableName, colName)
                    }
                    val newType = columnTypeRes.getString("data_type")
                    if (!desiredSqlType.startsWith(newType)) {
                        log.warn("Column $tableName.$colName did not really change the type to $desiredSqlType, stayed $newType.")
                    }
                }
            }
            catch (ex: SQLException) {
                log.error("Error changing type of column $tableName.$colName to $desiredSqlType.\n  ${ex.message}")
            }
            catch (ex: HsqlDbHelper.ColumnNotFoundException) {
                continue
            }
        }

        hsqlDbHelper.attachTable(tableName)
    }

    companion object { private val log = logger()}
}

/** Not yet used. */
data class SqlColumnType(val ddlDescription: String)
enum class SqlColumnTypeGroup(val types: List<SqlColumnType>) {
    STRINGS(listOf(SqlColumnType("LONGVARCHAR"))),
    NUMERIC_REAL(listOf(SqlColumnType("DECIMAL"), SqlColumnType("DECIMAL(14,6)"), SqlColumnType("DECIMAL(10,3)"), SqlColumnType("DECIMAL(1,2)"))),
    NUMERIC_INT(listOf(SqlColumnType("BIGINT"), SqlColumnType("INTEGER"), SqlColumnType("SMALLINT"))),
    TEMPORAL(listOf(SqlColumnType("TIMESTAMP"))),
    SPECIAL(listOf(SqlColumnType("UUID"))),
}
package cz.dynawest.csvcruncher;

import cz.dynawest.csvcruncher.util.DbUtils;
import cz.dynawest.csvcruncher.util.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

@Slf4j
public class HsqlDbHelper
{
    private static final Logger LOG = log;
    public static final int MAX_STRING_COLUMN_LENGTH = 4092;


    private Connection jdbcConn;

    public HsqlDbHelper(Connection jdbcConn)
    {
        this.jdbcConn = jdbcConn;
    }


    void createTableForInputFile(String tableName, File csvFileToBind, List<String> colNames, boolean ignoreFirst, boolean overwrite) throws SQLException
    {
        createTableAndBindCsv(tableName, csvFileToBind, colNames, ignoreFirst, "", true, overwrite);
    }

    private void createTableAndBindCsv(String tableName, File csvFileToBind, List<String> columnsNames, boolean ignoreFirst, String counterColumnDdl, boolean isInputTable, boolean overwrite) throws SQLException
    {
        Map<String, String> columnsDef = listToMapKeysWithNullValues(columnsNames);
        createTableAndBindCsv(tableName, csvFileToBind, columnsDef, ignoreFirst, counterColumnDdl, isInputTable, overwrite);

        // Try to convert columns types to numbers, where applicable.
        if (isInputTable) {
            this.optimizeTableCoumnsType(tableName, columnsNames);
        }
    }

    /**
     * Returns a map with keys from the given list, and null values. Doesn't deal with duplicate keys.
     */
    private static Map<String, String> listToMapKeysWithNullValues(List<String> keys)
    {
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        for (String columnsName : keys) {
            result.put(columnsName, null);
        }
        return result;
    }

    /**
     * Creates the input or output table, with the right column names, and binds the file.<br/>
     * For output tables, the file is optionally overwritten if exists.<br/>
     * A header with columns names is added to the output table.<br/>
     * Input tables columns are optimized after binding the file by attempting to reduce the column type.
     * (The output table has to be optimized later.)<br/>
     */
    void createTableAndBindCsv(String tableName, File csvFileToBind, Map<String, String> columnsNamesAndTypes, boolean ignoreFirst, String counterColumnDdl, boolean isInputTable, boolean overwrite) throws SQLException
    {
        boolean readOnly = false;
        boolean csvUsesSingleQuote = true;

        if (isInputTable && !csvFileToBind.exists()) {
            throw new IllegalArgumentException("The input file does not exist: " + csvFileToBind);
        }

        // Get a full path, because HSQLDB resolves paths against the data dir specified in JDBC URL.
        try {
            csvFileToBind = csvFileToBind.getCanonicalFile();
        }
        catch (IOException ex) {
            throw new CsvCruncherException("Failed resolving the CSV file path: " + csvFileToBind, ex);
        }

        // Delete any file at the output path, if exists. Other option would be to TRUNCATE, but this is safer.
        if (!isInputTable) {
            if (csvFileToBind.exists()) {
                if (true || overwrite) // TODO: Obey --overwrite.
                    csvFileToBind.delete();
                else
                    throw new IllegalArgumentException("The output file already exists. Use --overwrite or delete: " + csvFileToBind);
            }
            else {
                try {
                    Files.createDirectories(csvFileToBind.getParentFile().toPath());
                }
                catch (IOException ex) {
                    throw new CsvCruncherException("Failed creating directory to store the output to: " + csvFileToBind.getParentFile(), ex);
                }
            }
        }



        // We are also building a header for the CSV file.
        StringBuilder sbCsvHeader = new StringBuilder("# ");
        StringBuilder sbSql = (new StringBuilder("CREATE TEXT TABLE ")).append(tableName).append(" ( ");

        // The counter column, if any.
        sbSql.append(counterColumnDdl);

        // Columns
        for (Map.Entry<String, String> columnDef : columnsNamesAndTypes.entrySet())
        {
            String columnName = Utils.escapeSql(columnDef.getKey());
            String columnType = columnDef.getValue();
            if (columnType == null || "VARCHAR".equals(columnType.toUpperCase()))
                columnType = "VARCHAR(" + MAX_STRING_COLUMN_LENGTH + ")";
            else
                columnType = Utils.escapeSql(columnType);

            sbCsvHeader.append(columnName).append(", ");
            sbSql.append(columnName).append(" ").append(columnType).append(", ");
        }
        sbCsvHeader.delete(sbCsvHeader.length() - 2, sbCsvHeader.length());
        sbSql.delete(sbSql.length() - 2, sbSql.length());
        sbSql.append(" )");
        LOG.debug("Table DDL SQL: " + sbSql.toString());
        this.executeDbCommand(sbSql.toString(), "Failed to CREATE TEXT TABLE: ");


        // Bind the table to the CSV file.
        String csvPath = csvFileToBind.getPath();
        csvPath = Utils.escapeSql(csvPath);
        String quoteCharacter = csvUsesSingleQuote ? "\\quote" : "\"";
        String ignoreFirstFlag = ignoreFirst ? "ignore_first=true;" : "";
        String csvSettings = "encoding=UTF-8;cache_rows=50000;cache_size=10240000;" + ignoreFirstFlag + "fs=,;qc=" + quoteCharacter;
        String DESC = readOnly ? "DESC" : "";  // Not a mistake, HSQLDB really has "DESC" here for read only.
        String sql = String.format("SET TABLE %s SOURCE '%s;%s' %s", tableName, csvPath, csvSettings, DESC);
        LOG.debug("CSV import SQL: " + sql);
        this.executeDbCommand(sql, "Failed to import CSV: ");

        // SET TABLE <table name> SOURCE HEADER
        if (!isInputTable) {
            sql = String.format("SET TABLE %s SOURCE HEADER '%s'", tableName, sbCsvHeader.toString());
            LOG.debug("CSV source header SQL: " + sql);
            this.executeDbCommand(sql, "Failed to set CSV header: ");
        }

    }


    /**
     * Execute a SQL which does not expect a ResultSet,
     * and help the user with the common errors by parsing the message
     * and printing out some helpful info in a wrapping exception.
     *
     * @return the number of affected rows.
     */
    public int executeDbCommand(String sql, String errorMsg)
    {
        try (Statement stmt = this.jdbcConn.createStatement()){
            return stmt.executeUpdate(sql);
        }
        catch (Exception ex) {
            String addToMsg = "";
            //if (ex.getMessage().contains("for cast"))
            {
                // List column names with types.
                addToMsg = "\n  Tables and column types:\n"
                        + this.formatListOfAvailableTables(true);
            }

            if (ex.getMessage().contains("cannot be converted to target type")) {
                errorMsg = StringUtils.defaultString(errorMsg) + " Looks like the data in the input files do not match.";
            }

            if (StringUtils.isBlank(errorMsg))
                errorMsg = "Looks like there was a data type mismatch. Check the output table column types and your SQL.";

            throw new CsvCruncherException(errorMsg
                    + "\n  SQL: " + sql
                    + "\n  DB error: " + ex.getClass().getSimpleName() + " " + ex.getMessage()
                    + addToMsg
            );
        }
    }

    /**
     * Prepares a list of tables in the given JDBC connections, in the PUBLIC schema.
     */
    public String formatListOfAvailableTables(boolean withColumns)
    {
        String schema = "'PUBLIC'";

        StringBuilder sb = new StringBuilder();
        String sqlTablesMetadata =
                "SELECT table_name AS t, c.column_name AS c, c.data_type AS ct" +
                        " FROM INFORMATION_SCHEMA.TABLES AS t " +
                        " NATURAL JOIN INFORMATION_SCHEMA.COLUMNS AS c " +
                        " WHERE t.table_schema = " + schema;

        try (Statement st = jdbcConn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = st.executeQuery(sqlTablesMetadata);

            tables:
            while(rs.next()) {
                String tableName = rs.getString("T");
                sb.append(" * ").append(tableName).append('\n');
                while(StringUtils.equals(tableName, rs.getString("T"))) {
                    if (withColumns)
                        sb.append("    - ")
                                .append(StringUtils.rightPad(rs.getString("C"), 28))
                                .append(" ")
                                .append(rs.getString("CT"))
                                .append('\n');
                    if (!rs.next())
                        break tables;
                }
                rs.previous();
            }
            if (sb.length() == 0)
                return "    (No tables)";
            return sb.toString();
        }
        catch (SQLException ex) {
            String msg = "Failed listing tables: " + ex.getMessage();
            LOG.error(msg);
            return msg;
        }
    }

    /**
     * Analyzes the exception against the given DB connection and rethrows an exception with a message containing the available objects as a hint.
     */
    public CsvCruncherException throwHintForObjectNotFound(SQLSyntaxErrorException ex)
    {
        boolean notFoundIsColumn = DbUtils.analyzeWhatWasNotFound(ex.getMessage());

        String tableNames = formatListOfAvailableTables(notFoundIsColumn);

        String hintMsg = notFoundIsColumn ?
                "\n  Looks like you are referring to a column that is not present in the table(s).\n"
                        + "  Check the header (first line) in the CSV.\n"
                        + "  Here are the tables and columns are actually available:\n"
                :
                "\n  Looks like you are referring to a table that was not created.\n"
                        + "  This could mean that you have a typo in the input file name,\n"
                        + "  or maybe you use --combineInputs but try to use the original inputs.\n"
                        + "  These tables are actually available:\n";

        return new CsvCruncherException(
                hintMsg
                        + tableNames + "\nMessage from the database:\n  "
                        + ex.getMessage(), ex);
    }

    /**
     * Get the columns info: Perform the SQL, LIMIT 1.
     */
    public Map<String, String> extractColumnsInfoFrom1LineSelect(String sql) throws SQLException
    {
        PreparedStatement statement;
        try {
            statement = jdbcConn.prepareStatement(sql + " LIMIT 1");
        }
        catch (SQLSyntaxErrorException ex) {
            if (ex.getMessage().contains("object not found:")) {
                throw throwHintForObjectNotFound(ex);
            }
            throw new CsvCruncherException("Seems your SQL contains errors:\n" + ex.getMessage(), ex);
        }
        catch (SQLException ex) {
            throw new CsvCruncherException("Failed executing the SQL:\n" + ex.getMessage(), ex);
        }
        ResultSet rs = statement.executeQuery();

        // Column names
        return DbUtils.getResultSetColumnNamesAndTypes(rs);
    }


    /**
     * Detaches or re-attaches HSQLDB TEXT table.
     * @param drop     Drop the table after detaching.
     */
    void detachTable(String tableName, boolean drop) throws SQLException
    {
        LOG.debug(String.format("Detaching%s table: %s", drop ? " and dropping" : "", tableName));

        String sql = "SET TABLE " + Utils.escapeSql(tableName) + " SOURCE OFF";
        executeDbCommand(sql, "Failed to detach/attach the table: ");

        if (drop) {
            sql = "DROP TABLE " + Utils.escapeSql(tableName);
            executeDbCommand(sql, "Failed to DROP TABLE: ");
        }
    }

    void attachTable(String tableName) throws SQLException
    {
        LOG.debug("Ataching table: " + tableName);

        String sql = "SET TABLE " + Utils.escapeSql(tableName) + " SOURCE ON";
        executeDbCommand(sql, "Failed to attach the table: ");
    }

    /**
     * This must be called when all data are already in the table!
     * Try to convert columns to best fitting types.
     * This speeds up further SQL operations.
     *
     *   "HyperSQL allows changing the type if all the existing values can be cast
     *    into the new type without string truncation or loss of significant digits."
     */
    void optimizeTableCoumnsType(String tableName, List<String> colNames) throws SQLException
    {
        Map<String, String> columnsFitIntoType = new LinkedHashMap<>();

        // TODO: This doesn't work because: Operation is not allowed on text table with data in statement.
        // See https://stackoverflow.com/questions/52647738/hsqldb-hypersql-changing-column-type-in-a-text-table
        // Maybe I need to duplicate the TEXT table into a native table first?
        for (String colName : colNames)
        {
            String typeUsed;
            for (String sqlType : new String[]{"VARCHAR(255)", "CHAR", "TIMESTAMP", "UUID", "BIGINT", "INTEGER", "SMALLINT", "BOOLEAN"})
            {
                // Try CAST( AS ...)
                String sqlCol = String.format("SELECT CAST(%s AS %s) FROM %s", colName, sqlType, tableName);
                //String sqlCol = String.format("SELECT 1 + \"%s\" FROM %s", colName, tableName);

                LOG.trace("Column change attempt SQL: " + sqlCol);
                try (Statement st = jdbcConn.createStatement()) {
                    st.execute(sqlCol);
                    LOG.trace(String.format("Column %s.%s fits to %s", tableName, colName, typeUsed = sqlType));
                    columnsFitIntoType.put(colName, sqlType);
                }
                catch (SQLException ex) {
                    // LOG.trace(String.format("Column %s.%s values don't fit to %s.\n  %s", tableName, colName, sqlType, ex.getMessage()));
                }
            }
        }

        detachTable(tableName, false);

        // ALTER COLUMNs
        for (Map.Entry<String, String> colNameAndType : columnsFitIntoType.entrySet())
        {
            String colName = colNameAndType.getKey();
            String sqlType = colNameAndType.getValue();
            String sqlAlter = String.format("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s", tableName, colName, sqlType);
            String sqlCheck = String.format("SELECT data_type FROM information_schema.columns WHERE LOWER(table_name) = LOWER('%s') AND LOWER(column_name) = LOWER('%s')", tableName, colName);

            LOG.trace("Changing the column {} to {}", colName, sqlType);
            try (Statement st = jdbcConn.createStatement()) {
                st.execute(sqlAlter);
                LOG.info(String.format("Column %s.%s converted to %s. %s", tableName, colName, sqlType, sqlAlter));
                LOG.trace("Checking col type: " + sqlCheck);
                ResultSet columnTypeRes = st.executeQuery(sqlCheck);
                if (!columnTypeRes.next()) {
                    LOG.error("Column not found?? {}.{}", tableName, colName);
                    DbUtils.testDumpSelect("SELECT table_name, column_name FROM information_schema.columns WHERE LOWER(table_name) = LOWER('"+tableName.toUpperCase()+"')", jdbcConn);
                    continue;
                }
                String newType = columnTypeRes.getString("data_type");
                if (!newType.equals(sqlType) ) {
                    LOG.error(String.format("Column %s.%s did not really change the type to %s, stayed %s.", tableName, colName, sqlType, newType));
                }
            }
            catch (SQLException ex) {
                LOG.error(String.format("Error changing type of column %s.%s to %s.\n  %s", tableName, colName, sqlType, ex.getMessage()));
            }
        }

        attachTable(tableName);
    }

    public void detachTables(Set<String> tableNames, String msgOnError)
    {
        for (String tableName : tableNames) {
            try {
                detachTable(tableName, true);
            } catch (Exception ex) {
                LOG.error(msgOnError + ex.getMessage());
            }
        }

    }


    static String normalizeFileNameForTableName(File fileName)
    {
        return fileName.getName().replaceFirst(".csv$", "").replaceAll("[^a-zA-Z0-9_]", "_");
    }

}

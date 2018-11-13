package cz.dynawest.csvcruncher;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.HashMap;
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


    /**
     * Creates the input or output table, with the right column names, and binds the file.<br/>
     * For output tables, the file is optionally overwritten if exists.<br/>
     * A header with columns names is added to the output table.<br/>
     * Input tables columns are optimized after binding the file by attempting to reduce the column type.
     * (The output table has to be optimized later.)<br/>
     */
    void createTableAndBindCsv(String tableName, File csvFileToBind, List<String> colNames, boolean ignoreFirst, String counterColumnDdl, boolean isInputTable, boolean overwrite) throws SQLException
    {
        boolean readOnly = false;
        boolean csvUsesSingleQuote = true;

        // Delete any file at the output path, if exists. Other option would be to TRUNCATE, but this is safer.
        if ((!isInputTable) && csvFileToBind.exists())
            if (true || overwrite) // TODO: Obey --overwrite.
                csvFileToBind.delete();
            else
                throw new IllegalArgumentException("The output file already exists. Use --overwrite or delete: " + csvFileToBind);

        // We are also building a header for the CSV file.
        StringBuilder sbCsvHeader = new StringBuilder("# ");
        StringBuilder sb = (new StringBuilder("CREATE TEXT TABLE ")).append(tableName).append(" ( ");

        // The counter column, if any.
        sb.append(counterColumnDdl);

        // Columns
        for (String colName : colNames)
        {
            sbCsvHeader.append(colName).append(", ");

            colName = Utils.escapeSql(colName);
            sb.append(colName).append(" VARCHAR(" + MAX_STRING_COLUMN_LENGTH + "), ");
        }
        sbCsvHeader.delete(sbCsvHeader.length() - 2, sbCsvHeader.length());
        sb.delete(sb.length() - 2, sb.length());
        sb.append(" )");
        LOG.info("Table DDL SQL: " + sb.toString());
        this.executeDbCommand(sb.toString(), "Failed to CREATE TEXT TABLE: ");


        // Bind the table to the CSV file.
        String csvPath = csvFileToBind.getPath();
        csvPath = Utils.escapeSql(csvPath);
        String quoteCharacter = csvUsesSingleQuote ? "\\quote" : "\"";
        String ignoreFirstFlag = ignoreFirst ? "ignore_first=true;" : "";
        String csvSettings = "encoding=UTF-8;cache_rows=50000;cache_size=10240000;" + ignoreFirstFlag + "fs=,;qc=" + quoteCharacter;
        String DESC = readOnly ? "DESC" : "";  // Not a mistake, HSQLDB really has "DESC" here for read only.
        String sql = String.format("SET TABLE %s SOURCE '%s;%s' %s", tableName, csvPath, csvSettings, DESC);
        LOG.info("CSV import SQL: " + sql);
        this.executeDbCommand(sql, "Failed to import CSV: ");

        // SET TABLE <table name> SOURCE HEADER
        if (!isInputTable) {
            sql = String.format("SET TABLE %s SOURCE HEADER '%s'", tableName, sbCsvHeader.toString());
            LOG.info("CSV source header SQL: " + sql);
            this.executeDbCommand(sql, "Failed to set CSV header: ");
        }

        // Try to convert columns to numbers, where applicable.
        if (isInputTable) {
            this.optimizeTableCoumnsType(tableName, colNames);
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
                // List columns with types.
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
    public List<String> extractColumnsInfoFrom1LineSelect(String sql) throws SQLException
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
        return DbUtils.getResultSetColumnNames(rs);
    }


    /**
     * Detaches or re-attaches HSQLDB TEXT table.
     * @param drop     Drop the table after detaching.
     */
    void detachTable(String tableName, boolean drop) throws SQLException
    {
        LOG.info(String.format("Detaching%s table: %s", drop ? " and dropping" : "", tableName));

        String sql = "SET TABLE " + Utils.escapeSql(tableName) + " SOURCE OFF";
        executeDbCommand(sql, "Failed to detach/attach the table: ");

        if (drop) {
            sql = "DROP TABLE " + Utils.escapeSql(tableName);
            executeDbCommand(sql, "Failed to DROP TABLE: ");
        }
    }

    void attachTable(String tableName) throws SQLException
    {
        LOG.info("Ataching table: " + tableName);

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
        Map<String, String> columnsFitIntoType = new HashMap<>();

        // TODO: This doesn't work because: Operation is not allowed on text table with data in statement.
        // See https://stackoverflow.com/questions/52647738/hsqldb-hypersql-changing-column-type-in-a-text-table
        // Maybe I need to duplicate the TEXT table into a native table first?
        for (String colName : colNames)
        {
            String typeUsed;
            for (String sqlType : new String[]{"TIMESTAMP", "UUID", "BIGINT", "INTEGER", "SMALLINT", "BOOLEAN"})
            {
                // Try CAST( AS ...)
                String sqlCol = String.format("SELECT CAST(%s AS %s) FROM %s", colName, sqlType, tableName);
                //String sqlCol = String.format("SELECT 1 + \"%s\" FROM %s", colName, tableName);

                LOG.trace("Column change attempt SQL: " + sqlCol);
                try (Statement st = jdbcConn.createStatement()) {
                    st.execute(sqlCol);
                    LOG.debug(String.format("Column %s.%s fits to to %s", tableName, colName, typeUsed = sqlType));
                    columnsFitIntoType.put(colName, sqlType);
                }
                catch (SQLException ex) {
                    // LOG.info(String.format("Column %s.%s values don't fit to %s.\n  %s", tableName, colName, sqlType, ex.getMessage()));
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

            LOG.trace("Column change attempt SQL: " + sqlAlter);
            try (Statement st = jdbcConn.createStatement()) {
                st.execute(sqlAlter);
                LOG.debug(String.format("Column %s.%s converted to to %s", tableName, colName, sqlType));
            }
            catch (SQLException ex) {
                LOG.trace(String.format("Column %s.%s values don't fit to %s.\n  %s", tableName, colName, sqlType, ex.getMessage()));
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

package cz.dynawest.csvcruncher;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class Cruncher
{
    private static final Logger LOG = Logger.getLogger(Cruncher.class.getName());

    public static final String TABLE_NAME__OUTPUT = "output";
    public static final long TIMESTAMP_SUBSTRACT = 1_530_000_000_000L; // To make the unique ID a smaller number.
    public static final String FILENAME_SUFFIX_CSV = ".csv";
    public static final Pattern REGEX_SQL_COLUMN_VALID_NAME = Pattern.compile("[a-z][a-z0-9_]*", Pattern.CASE_INSENSITIVE);
    public static final int MAX_STRING_COLUMN_LENGTH = 4092;

    private Connection conn;
    private Options options;

    public Cruncher(Options options)
    {
        this.options = options;
        this.init();
    }

    private void init()
    {
        System.setProperty("textdb.allow_full_path", "true");
        //System.setProperty("hsqldb.reconfig_logging", "false");

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        }
        catch (ClassNotFoundException e) {
            throw new CsvCruncherException("Couldn't find JDBC driver: " + e.getMessage(), e);
        }

        String dbPath = StringUtils.defaultIfEmpty(this.options.dbPath, "hsqldb") + "/cruncher";
        try {
            FileUtils.forceMkdir(new File(dbPath));
            this.conn = DriverManager.getConnection("jdbc:hsqldb:file:" + dbPath + ";shutdown=true", "SA", "");
        }
        catch (IOException e) {
            throw new CsvCruncherException(String.format("Can't create HSQLDB data dir %s: %s", dbPath, e.getMessage()), e);
        }
        catch (SQLException e) {
            throw new CsvCruncherException(String.format("Can't connect to the database %s: %s", dbPath, e.getMessage()), e);
        }
    }

    /**
     * Performs the whole process.
     */
    public void crunch() throws Exception
    {
        validateParameters();

        Map<String, File> tablesToFiles = new HashMap<>();
        try
        {
            boolean addCounterColumn = options.initialRowNumber != null;
            boolean convertResultToJson = options.jsonExportFormat != Options.JsonExportFormat.NONE;
            boolean printAsArray = options.jsonExportFormat == Options.JsonExportFormat.ARRAY;

            ReachedCrunchStage reachedStage = ReachedCrunchStage.NONE;

            File csvOutFile = Utils.resolvePathToUserDirIfRelative(Paths.get(this.options.outputPathCsv));
            csvOutFile.getAbsoluteFile().getParentFile().mkdirs();

            try
            {
                // Sort the input paths.
                List<Path> inputPaths = this.options.inputPaths.stream().map(Paths::get).collect(Collectors.toList());
                inputPaths = FilesUtils.sortInputPaths(inputPaths, this.options.sortInputFiles);
                LOG.info(" --- Sorted input paths: --- " + inputPaths.stream().map(p -> "\n * "+ p).reduce(String::concat).get());

                // Combine files. Should we concat the files or UNION the tables?
                if (this.options.combineInputFiles != Options.CombineInputFiles.NONE) {
                    List<Path> concatenatedFiles = FilesUtils.combineInputFiles(inputPaths, this.options);
                    inputPaths = concatenatedFiles;
                    LOG.info(" --- Combined input files: --- " + inputPaths.stream().map(p -> "\n * "+ p).reduce(String::concat).orElse("NONE"));
                    reachedStage = ReachedCrunchStage.INPUT_FILES_PREPROCESSED;
                }

                if (inputPaths.isEmpty())
                    return;


                //LOG.info(" --- ================================================================ --- ");

                // For each input CSV file...
                for (Path path : inputPaths) {
                    File csvInFile = Utils.resolvePathToUserDirIfRelative(path);
                    LOG.info(" * CSV input: " + csvInFile);

                    String tableName = normalizeFileNameForTableName(csvInFile);
                    File previousIfAny = tablesToFiles.put(tableName, csvInFile);
                    if (previousIfAny != null)
                        throw new IllegalArgumentException("File names normalized to table names collide: " + previousIfAny + ", " + csvInFile);

                    List<String> colNames = FilesUtils.parseColsFromFirstCsvLine(csvInFile);
                    // Create a table and bind the CSV to it.
                    this.createTableForInputFile(tableName, csvInFile, colNames, true);
                }
                reachedStage = ReachedCrunchStage.INPUT_TABLES_CREATED;


                // Should the result have a unique incremental ID as an added 1st column?
                CounterColumn counterColumn = new CounterColumn();
                if (addCounterColumn)
                    counterColumn.setDdlAndVal();

                // Get the columns info: Perform the SQL, LIMIT 1.
                List<String> colNames = extractColumnsInfoFrom1LineSelect(this.options.sql);

                // Write the result into a CSV
                LOG.info(" * CSV output: " + csvOutFile);
                this.createTableAndBindCsv(TABLE_NAME__OUTPUT, csvOutFile, colNames, true, counterColumn.ddl, false);
                reachedStage = ReachedCrunchStage.OUTPUT_TABLE_CREATED;


                // The provided SQL could be something like "SELECT @counter, foo, bar FROM ..."
                //String selectSql = this.options.sql.replace("@counter", value);
                // On the other hand, that's too much space for the user to screw up. Let's force it:
                String selectSql = this.options.sql.replace("SELECT ", "SELECT " + counterColumn.value + " ");

                String userSql = "INSERT INTO " + TABLE_NAME__OUTPUT + " (" + selectSql + ")";
                LOG.info(" * User's SQL: " + userSql);
                //LOG.info("\n  Tables and column types:\n" + this.formatListOfAvailableTables(true));///
                int rowsAffected = executeDbCommand(userSql, "Error executing user SQL: ");
                reachedStage = ReachedCrunchStage.OUTPUT_TABLE_FILLED;


                // Now let's convert it to JSON if necessary.
                if (convertResultToJson) {
                    Path destJsonFile = Paths.get(csvOutFile.toPath().toString() + ".json");
                    LOG.info(" * JSON output: " + destJsonFile);

                    try (Statement statement2 = this.conn.createStatement()) {
                        FilesUtils.convertResultToJson(
                                statement2.executeQuery("SELECT * FROM " + TABLE_NAME__OUTPUT),
                                destJsonFile,
                                printAsArray
                        );
                        if (!this.options.keepWorkFiles)
                            csvOutFile.deleteOnExit();
                    }
                    reachedStage = ReachedCrunchStage.OUTPUT_JSON_CONVERTED;
                }
            }
            catch (Exception ex) {
                throw new CsvCruncherException("(DB tables and files cleanup was performed.)", ex);
            }
            finally
            {
                LOG.info(" *** SHUTDOWN CLEANUP SEQUENCE ***");
                cleanUpInputOutputTables(tablesToFiles);
                executeDbCommand("DROP SCHEMA PUBLIC CASCADE", "Failed to delete the database: ");
                this.conn.close();
                LOG.info(" *** END SHUTDOWN CLEANUP SEQUENCE ***");
            }
        }
        catch (Exception ex) {
            throw ex;
        }
    }

    /**
     * Get the columns info: Perform the SQL, LIMIT 1.
     */
    private List<String> extractColumnsInfoFrom1LineSelect(String sql) throws SQLException
    {
        PreparedStatement statement;
        try {
            statement = this.conn.prepareStatement(sql + " LIMIT 1");
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

    private CsvCruncherException throwHintForObjectNotFound(SQLSyntaxErrorException ex)
    {
        boolean notFoundIsColumn = DbUtils.analyzeWhatWasNotFound(ex.getMessage(), this.options.sql);

        String tableNames = formatListOfAvailableTables(notFoundIsColumn); // TODO

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

    private void cleanUpInputOutputTables(Map<String, File> inputTablesToFiles)
    {
        //if (reachedStage.passed(ReachedCrunchStage.INPUT_TABLES_CREATED))
        // I'm removing these stage checks, since the table might have been left
        // from previous run. Later let's implement a cleanup at start. TODO
        {
            for (Map.Entry<String, File> tableAndFile : inputTablesToFiles.entrySet()) {
                try {
                    this.detachTable(tableAndFile.getKey(), false, true);
                } catch (Exception ex) {
                    LOG.severe("Could not delete the input table: " + ex.getMessage());
                }
            }
        }

        //if (reachedStage.passed(ReachedCrunchStage.OUTPUT_TABLE_CREATED))
        {
            try {
                this.detachTable(TABLE_NAME__OUTPUT, false, true);
            } catch (Exception ex) {
                LOG.fine("Could not delete the output table: " + ex.getMessage());
            }
        }

        //if (reachedStage.passed(ReachedCrunchStage.OUTPUT_TABLE_FILLED))
    }

    enum ReachedCrunchStage {
        NONE,
        INPUT_FILES_PREPROCESSED,
        INPUT_TABLES_CREATED,
        OUTPUT_TABLE_CREATED,
        OUTPUT_TABLE_FILLED,
        OUTPUT_JSON_CONVERTED;

        public boolean passed(ReachedCrunchStage stage)
        {
            LOG.info(String.format("%s >= %s?", this.ordinal(), stage.ordinal()));
            return (this.ordinal() >= stage.ordinal());
        }
    }

    private String formatListOfAvailableTables(boolean withColumns)
    {
        StringBuilder sb = new StringBuilder();

        String sqlTablesMetadata =
                "SELECT table_name AS t, c.column_name AS c, c.data_type AS ct" +
                    " FROM INFORMATION_SCHEMA.TABLES AS t " +
                    " NATURAL JOIN INFORMATION_SCHEMA.COLUMNS AS c " +
                " WHERE t.table_schema = 'PUBLIC'";

        try (Statement st = this.conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            ResultSet rs = st.executeQuery(sqlTablesMetadata);

            tables:
            while(rs.next()) {
                String tableName = rs.getString("T");
                sb.append(" * ").append(tableName).append('\n');
                while(tableName == rs.getString("T")) {
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
            LOG.severe(msg);
            return msg;
        }
    }

    private long getInitialNumber()
    {
        long initialNumber;

        if (options.initialRowNumber != -1) {
            initialNumber = options.initialRowNumber;
        } else {
            // A timestamp at the beginning:
            //sql = "DECLARE crunchCounter BIGINT DEFAULT UNIX_MILLIS() - 1530000000000";
            //executeDbCommand(sql, "Failed creating the counter variable: ");
            // Uh oh. Variables can't be used in SELECTs.
            initialNumber = (System.currentTimeMillis() - TIMESTAMP_SUBSTRACT);
        }
        return initialNumber;
    }


    /**
     * Execute a SQL which does not expect a ResultSet,
     * and help the user with the common errors by parsing the message
     * and printing out some helpful info in a wrapping exception.
     *
     * @return the number of affected rows.
     */
    private int executeDbCommand(String sql, String errorMsg) throws SQLException
    {
        try (Statement stmt = this.conn.createStatement()){
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

    private static String normalizeFileNameForTableName(File fileName)
    {
        return fileName.getName().replaceFirst(".csv$", "").replaceAll("[^a-zA-Z0-9_]", "_");
    }

    private void validateParameters() throws FileNotFoundException
    {
        if (this.options.inputPaths == null || this.options.inputPaths.isEmpty())
            throw new IllegalArgumentException(" -in is not set.");

        if (this.options.sql == null)
            throw new IllegalArgumentException(" -sql is not set.");

        if (this.options.outputPathCsv == null)
            throw new IllegalArgumentException(" -out is not set.");


        for (String path : this.options.inputPaths) {
            File ex = new File(path);
            if (!ex.exists())
                throw new FileNotFoundException("CSV file not found: " + ex.getPath());
        }
    }

    private void createTableForInputFile(String tableName, File csvFileToBind, List<String> colNames, boolean ignoreFirst) throws SQLException, FileNotFoundException
    {
        createTableAndBindCsv(tableName, csvFileToBind, colNames, ignoreFirst, "", true);
    }


    /**
     * Creates the input or output table, with the right column names, and binds the file.<br/>
     * For output tables, the file is optionally overwritten if exists.<br/>
     * A header with columns names is added to the output table.<br/>
     * Input tables columns are optimized after binding the file by attempting to reduce the column type.
     * (The output table has to be optimized later.)<br/>
     */
    private void createTableAndBindCsv(String tableName, File csvFileToBind, List<String> colNames, boolean ignoreFirst, String counterColumnDdl, boolean input) throws SQLException
    {
        boolean readOnly = false;
        boolean csvUsesSingleQuote = true;

        // Delete any file at the output path, if exists. Other option would be to TRUNCATE, but this is safer.

        if ((!input) && csvFileToBind.exists())
            if (true || options.overwrite) // TODO: Obey --overwrite.
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
        executeDbCommand(sb.toString(), "Failed to CREATE TEXT TABLE: ");


        // Bind the table to the CSV file.
        String csvPath = csvFileToBind.getPath();
        csvPath = Utils.escapeSql(csvPath);
        String quoteCharacter = csvUsesSingleQuote ? "\\quote" : "\"";
        String ignoreFirstFlag = ignoreFirst ? "ignore_first=true;" : "";
        String csvSettings = "encoding=UTF-8;cache_rows=50000;cache_size=10240000;" + ignoreFirstFlag + "fs=,;qc=" + quoteCharacter;
        String DESC = readOnly ? "DESC" : "";  // Not a mistake, HSQLDB really has "DESC" here for read only.
        String sql = String.format("SET TABLE %s SOURCE '%s;%s' %s", tableName, csvPath, csvSettings, DESC);
        LOG.info("CSV import SQL: " + sql);
        executeDbCommand(sql, "Failed to import CSV: ");

        // SET TABLE <table name> SOURCE HEADER
        if (!input) {
            sql = String.format("SET TABLE %s SOURCE HEADER '%s'", tableName, sbCsvHeader.toString());
            LOG.info("CSV source header SQL: " + sql);
            executeDbCommand(sql, "Failed to set CSV header: ");
        }

        // Try to convert columns to numbers, where applicable.
        if (input) {
            optimizeTableCoumnsType(tableName, colNames);
        }
    }

    /**
     * This must be called when all data are already in the table!
     * Try to convert columns to best fitting types.
     * This speeds up further SQL operations.
     *
     // "HyperSQL allows changing the type if all the existing values can be cast
     // into the new type without string truncation or loss of significant digits."
     */
    private void optimizeTableCoumnsType(String tableName, List<String> colNames) throws SQLException
    {
        Map<String, String> columnsFitIntoType = new HashMap<>();

        // TODO: This doesn't work because: Operation is not allowed on text table with data in statement.
        // See https://stackoverflow.com/questions/52647738/hsqldb-hypersql-changing-column-type-in-a-text-table
        // Maybe I need to duplicate the TEXT table into a native table first?
        for (String colName : colNames) {
            String typeUsed = null;
            for (String sqlType : new String[]{"TIMESTAMP", "UUID", "BIGINT", "INTEGER", "SMALLINT", "BOOLEAN"}) {
                // Try CAST( AS ...)
                String sqlCol = String.format("SELECT CAST(%s AS %s) FROM %s", colName, sqlType, tableName);
                //String sqlCol = String.format("SELECT 1 + \"%s\" FROM %s", colName, tableName);

                LOG.finer("Column change attempt SQL: " + sqlCol);
                try (Statement st = this.conn.createStatement()) {
                    st.execute(sqlCol);
                    LOG.fine(String.format("Column %s.%s fits to to %s", tableName, colName, typeUsed = sqlType));
                    columnsFitIntoType.put(colName, sqlType);
                }
                catch (SQLException ex) {
                    // LOG.info(String.format("Column %s.%s values don't fit to %s.\n  %s", tableName, colName, sqlType, ex.getMessage()));
                }
            }
        }

        detachTable(tableName, false, false);

        // ALTER COLUMNs
        for (Map.Entry<String, String> colNameAndType : columnsFitIntoType.entrySet()) {
            String colName = colNameAndType.getKey();
            String sqlType = colNameAndType.getValue();
            String sqlAlter = String.format("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s", tableName, colName, sqlType);

            LOG.finer("Column change attempt SQL: " + sqlAlter);
            try (Statement st = this.conn.createStatement()) {
                st.execute(sqlAlter);
                LOG.fine(String.format("Column %s.%s converted to to %s", tableName, colName, sqlType));
            }
            catch (SQLException ex) {
                LOG.finer(String.format("Column %s.%s values don't fit to %s.\n  %s", tableName, colName, sqlType, ex.getMessage()));
            }
        }

        detachTable(tableName, true, false);
    }

    private void detachTable(String tableName, boolean reattach, boolean drop) throws SQLException
    {
        if (reattach)
            LOG.info("Re-ataching table: " + tableName);
        else
            LOG.info(String.format("Deataching%s table: %s", drop ? " and dropping" : "", tableName));


        String sql = "SET TABLE " + Utils.escapeSql(tableName) + " SOURCE " + (reattach ? "ON" : "OFF");
        executeDbCommand(sql, "Failed to detach/attach the table: ");

        if (drop) {
            sql = "DROP TABLE " + Utils.escapeSql(tableName);
            executeDbCommand(sql, "Failed to DROP TABLE: ");
        }
    }


    /**
     * This is for the case we use hand-made JSON marshalling.
     * Returns null if the column value was null, or if the returned type is not supported.
     */
    private static String formatValueForJson(ResultSet resultSet, int colIndex, boolean[] colsAreNumbers) throws SQLException
    {
        ResultSetMetaData metaData = resultSet.getMetaData();
        String val;
        switch (metaData.getColumnType(colIndex)) {
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.CLOB:
                val = resultSet.getString(colIndex); break;
            case Types.TINYINT:
            case Types.BIT:
                val = (""+resultSet.getByte(colIndex)); break;
            case Types.SMALLINT: val = (""+resultSet.getShort(colIndex)); break;
            case Types.INTEGER:  val = (""+resultSet.getInt(colIndex)); break;
            case Types.BIGINT:   val = (""+resultSet.getLong(colIndex)); break;
            case Types.BOOLEAN:  val = (""+resultSet.getBoolean(colIndex)); break;
            case Types.FLOAT:    val = (""+resultSet.getFloat(colIndex)); break;
            case Types.DOUBLE:
            case Types.DECIMAL:  val = (""+resultSet.getDouble(colIndex)); break;
            case Types.NUMERIC:  val = (""+resultSet.getBigDecimal(colIndex)); break;
            case Types.DATE:    val = (""+resultSet.getDate(colIndex)); break;
            case Types.TIME:    val = (""+resultSet.getTime(colIndex)); break;
            case Types.TIMESTAMP:    val = (""+resultSet.getTimestamp(colIndex)).replace(' ', 'T');  break; // JS Date() takes "1995-12-17T03:24:00"
            default:
                LOG.severe("Unsupported type of column " + metaData.getColumnLabel(colIndex) + ": " + metaData.getColumnTypeName(colIndex));
                return null;
        }
        if (resultSet.wasNull())
            return null;
        return val;
    }


    private void testDumpSelect(String tableName) throws SQLException
    {
        PreparedStatement ps = this.conn.prepareStatement("SELECT * FROM " + tableName);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();

        while (rs.next())
        {
            System.out.println(" ------- ");

            for (int i = 1; i <= metaData.getColumnCount(); ++i)
            {
                System.out.println(" " + metaData.getColumnLabel(i) + ": " + rs.getObject(i));
            }
        }
    }

    private class CounterColumn
    {
        protected String ddl = "";
        protected String value = "";

        public CounterColumn setDdlAndVal() throws SQLException
        {
            long initialNumber = getInitialNumber();

            String sql;

            // Using an IDENTITY column which has an unnamed sequence?
            //ddl = "crunchCounter BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY";
            // ALTER TABLE output ALTER COLUMN crunchCounter RESTART WITH UNIX_MILLIS() - 1530000000000;
            // INSERT INTO otherTable VALUES (IDENTITY(), ...)

            // Or using a sequence?
            sql = "CREATE SEQUENCE IF NOT EXISTS crunchCounter AS BIGINT NO CYCLE"; // MINVALUE 1 STARTS WITH <number>
            executeDbCommand(sql, "Failed creating the counter sequence: ");

            sql = "ALTER SEQUENCE crunchCounter RESTART WITH " + initialNumber;
            executeDbCommand(sql, "Failed altering the counter sequence: ");

            // ... referencing it explicitely?
            //ddl = "crunchCounter BIGINT PRIMARY KEY, ";
            // INSERT INTO output VALUES (NEXT VALUE FOR crunchCounter, ...)
            //value = "NEXT VALUE FOR crunchCounter, ";

            // ... or using it through GENERATED BY?
            ddl = "crunchCounter BIGINT GENERATED BY DEFAULT AS SEQUENCE crunchCounter PRIMARY KEY, ";
            //value = "DEFAULT, ";
            value = "NULL AS crunchCounter, ";
            // INSERT INTO output (id, firstname, lastname) VALUES (DEFAULT, ...)
            // INSERT INTO otherTable VALUES (CURRENT VALUE FOR crunchCounter, ...)
            return this;
        }
    }
}

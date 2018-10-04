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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;

public class Cruncher
{
    private static final Logger LOG = Logger.getLogger(Cruncher.class.getName());

    public static final String TABLE_NAME__OUTPUT = "output";
    public static final long TIMESTAMP_SUBSTRACT = 1_530_000_000_000L; // To make the unique ID a smaller number.
    public static final String FILENAME_SUFFIX_CSV = ".csv";
    public static final Pattern REGEX_SQL_COLUMN_VALID_NAME = Pattern.compile("[a-z][a-z0-9_]*", Pattern.CASE_INSENSITIVE);
    public static final int MAX_STRING_COLUMN_LENGTH = 255;

    private Connection conn;
    private Options options;

    public Cruncher(Options options) throws ClassNotFoundException, SQLException
    {
        this.options = options;
        this.init();
    }

    private void init() throws ClassNotFoundException, SQLException
    {
        System.setProperty("textdb.allow_full_path", "true");
        //System.setProperty("hsqldb.reconfig_logging", "false");

        Class.forName("org.hsqldb.jdbc.JDBCDriver");
        String dbPath = StringUtils.defaultIfEmpty(this.options.dbPath, "hsqldb") + "/cruncher";
        try {
            FileUtils.forceMkdir(new File(dbPath));
        }
        catch (IOException e) {
            throw new RuntimeException(String.format("Can't create HSQLDB data dir %s: %s", dbPath, e.getMessage()));
        }
        this.conn = DriverManager.getConnection("jdbc:hsqldb:file:" + dbPath + ";shutdown=true", "SA", "");
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
                // Sort
                List<Path> inputPaths = this.options.inputPaths.stream().map(Paths::get).collect(Collectors.toList());
                inputPaths = FilesUtils.sortInputPaths(inputPaths, this.options.sortInputFiles);
                LOG.info(" --- Sorted input paths: --- " + inputPaths.stream().map(p -> "\n * "+ p).reduce(String::concat).get());

                // Combine files. Should we concat the files or UNION the tables?
                if (this.options.combineInputFiles != Options.CombineInputFiles.NONE) {
                    List<Path> concatenatedFiles = FilesUtils.combineInputFiles(inputPaths, this.options);
                    inputPaths = concatenatedFiles;
                    LOG.info(" --- Combined input files: --- " + inputPaths.stream().map(p -> "\n * "+ p).reduce(String::concat).get());
                    reachedStage = ReachedCrunchStage.INPUT_FILES_PREPROCESSED;
                }


                LOG.info(" --- ===================== --- ");

                // For each input CSV file...
                for (Path path : inputPaths) {
                    File csvInFile = Utils.resolvePathToUserDirIfRelative(path);
                    LOG.info(" * CSV input: " + csvInFile);

                    String tableName = normalizeFileNameForTableName(csvInFile);
                    File previousIfAny = tablesToFiles.put(tableName, csvInFile);
                    if (previousIfAny != null)
                        throw new IllegalArgumentException("File names normalized to table names collide: " + previousIfAny + ", " + csvInFile);

                    List<String> colNames = parseColsFromFirstLine(csvInFile);
                    // Read the CSV into a table.
                    this.createTableForCsvFile(tableName, csvInFile, colNames, true);
                }
                reachedStage = ReachedCrunchStage.INPUT_TABLES_CREATED;


                // Should the result have a unique incremental ID as an added 1st column?
                String counterColumnDdl = "";
                String counterColumnVal = "";

                if (addCounterColumn) {

                    long initialNumber = getInitialNumber();

                    String sql;

                    // Using an IDENTITY column which has an unnamed sequence?
                    //counterColumnDdl = "crunchCounter BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY";
                    // ALTER TABLE output ALTER COLUMN crunchCounter RESTART WITH UNIX_MILLIS() - 1530000000000;
                    // INSERT INTO otherTable VALUES (IDENTITY(), ...)

                    // Or using a sequence?
                    sql = "CREATE SEQUENCE IF NOT EXISTS crunchCounter AS BIGINT NO CYCLE"; // MINVALUE 1 STARTS WITH <number>
                    executeDbCommand(sql, "Failed creating the counter sequence: ");

                    sql = "ALTER SEQUENCE crunchCounter RESTART WITH " + initialNumber;
                    executeDbCommand(sql, "Failed altering the counter sequence: ");

                    // ... referencing it explicitely?
                    //counterColumnDdl = "crunchCounter BIGINT PRIMARY KEY, ";
                    // INSERT INTO output VALUES (NEXT VALUE FOR crunchCounter, ...)
                    //counterColumnVal = "NEXT VALUE FOR crunchCounter, ";

                    // ... or using it through GENERATED BY?
                    counterColumnDdl = "crunchCounter BIGINT GENERATED BY DEFAULT AS SEQUENCE crunchCounter PRIMARY KEY, ";
                    //counterColumnVal = "DEFAULT, ";
                    counterColumnVal = "NULL, ";
                    // INSERT INTO output (id, firstname, lastname) VALUES (DEFAULT, ...)
                    // INSERT INTO otherTable VALUES (CURRENT VALUE FOR crunchCounter, ...)
                }


                // Get the columns info: Perform the SQL, LIMIT 1.
                PreparedStatement statement;
                try {
                    statement = this.conn.prepareStatement(this.options.sql + " LIMIT 1");
                }
                catch (SQLSyntaxErrorException ex) {
                    if (ex.getMessage().contains("object not found:")) {

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

                        throw new RuntimeException(
                                hintMsg
                                + tableNames + "\nMessage from the database:\n  "
                                + ex.getMessage(), ex);
                    }
                    throw new RuntimeException("Seems your SQL contains errors:\n" + ex.getMessage(), ex);
                }
                ResultSet rs = statement.executeQuery();

                // Column names
                List<String> colNames = DbUtils.getResultSetColumnNames(rs);

                // Write the result into a CSV
                LOG.info(" * CSV output: " + csvOutFile);
                this.createTableForCsvFile(TABLE_NAME__OUTPUT, csvOutFile, colNames, true, counterColumnDdl, false);
                reachedStage = ReachedCrunchStage.OUTPUT_TABLE_CREATED;


                // The provided SQL could be something like "SELECT @counter, foo, bar FROM ..."
                //String selectSql = this.options.sql.replace("@counter", counterColumnVal);
                // On the other hand, that's too much space for the user to screw up. Let's force it:
                String selectSql = this.options.sql.replace("SELECT ", "SELECT " + counterColumnVal + " ");

                String userSql = "INSERT INTO " + TABLE_NAME__OUTPUT + " (" + selectSql + ")";
                LOG.info(" * User's SQL: " + userSql);
                statement = this.conn.prepareStatement(userSql);
                int rowsAffected = statement.executeUpdate();
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
                    }
                    reachedStage = ReachedCrunchStage.OUTPUT_JSON_CONVERTED;
                }
            }
            catch (Exception ex) {
                throw new RuntimeException("(DB tables and files cleanup was performed.)", ex);
            }
            finally
            {
                if (reachedStage.passed(ReachedCrunchStage.INPUT_TABLES_CREATED)) {
                    for (Map.Entry<String, File> tableAndFile : tablesToFiles.entrySet()) {
                        this.detachTable(tableAndFile.getKey(), false, true);
                    }
                }

                if (reachedStage.passed(ReachedCrunchStage.OUTPUT_TABLE_CREATED)) {
                    this.detachTable(TABLE_NAME__OUTPUT, false, true);
                }

                if (reachedStage.passed(ReachedCrunchStage.OUTPUT_TABLE_FILLED)) {
                    executeDbCommand("DROP SCHEMA PUBLIC CASCADE", "Failed to delete the database: ");
                    this.conn.close();
                }
            }
        }
        catch (Exception ex) {
            throw ex;
        }
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
            return (this.ordinal() >= stage.ordinal());
        }
    }

    private String formatListOfAvailableTables(boolean withColumns)
    {
        StringBuilder sb = new StringBuilder();

        String sql =
                "SELECT table_name AS t, column_name AS c " +
                    " FROM INFORMATION_SCHEMA.TABLES AS t " +
                    " NATURAL JOIN INFORMATION_SCHEMA.COLUMNS AS c " +
                " WHERE t.table_schema = 'PUBLIC'";

        try (ResultSet rs = this.conn.createStatement().executeQuery(sql)) {
            tables:
            while(rs.next()) {
                String tableName = rs.getString("T");
                sb.append(" * ").append(tableName).append('\n');
                while(tableName == rs.getString("T")) {
                    if (withColumns)
                        sb.append("    - ").append(rs.getString("C")).append('\n');
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


    private void executeDbCommand(String sql, String errorMsg) throws SQLException
    {
        try (Statement stmt = this.conn.createStatement()){
            stmt.execute(sql);
        } catch (Exception ex) {
            if (StringUtils.isBlank(errorMsg))
                throw ex;
            else
                throw new RuntimeException(errorMsg +
                        "\n  " + sql +
                        "\n  " + ex.getMessage() +
                        "\n  " + this.conn.getWarnings().getNextWarning(), ex);
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

    private static List<String> parseColsFromFirstLine(File file) throws IOException
    {
        Matcher mat = REGEX_SQL_COLUMN_VALID_NAME.matcher("");
        ArrayList<String> cols = new ArrayList();
        LineIterator lineIterator = FileUtils.lineIterator(file);
        if (!lineIterator.hasNext())
        {
            throw new IllegalStateException("No first line with columns definition (format: [# ] <colName> [, ...]) in: " + file.getPath());
        }
        else
        {
            String line = lineIterator.nextLine().trim();
            line = StringUtils.stripStart(line, "#");
            String[] colNames = StringUtils.splitPreserveAllTokens(line, ",;");

            for (String colName : Arrays.asList(colNames))
            {
                colName = colName.trim();
                if (colName.isEmpty())
                    throw new IllegalStateException(String.format(
                            "Empty column name (separators: ,; ) in: %s\n  The line was: %s",
                            file.getPath(), line
                    ));

                if (!mat.reset(colName).matches())
                    throw new IllegalStateException(String.format(
                            "Colname '%s' must be valid SQL identifier, i.e. must match /%s/i in: %s",
                            colName, REGEX_SQL_COLUMN_VALID_NAME.pattern(), file.getPath()
                    ));

                cols.add(colName);
            }

            return cols;
        }
    }

    private void createTableForCsvFile(String tableName, File csvFileToBind, List<String> colNames, boolean ignoreFirst) throws SQLException, FileNotFoundException
    {
        createTableForCsvFile(tableName, csvFileToBind, colNames, ignoreFirst, "", true);
    }


    private void createTableForCsvFile(String tableName, File csvFileToBind, List<String> colNames, boolean ignoreFirst, String counterColumnDdl, boolean readOnlyX) throws SQLException
    {
        boolean readOnly = false;
        boolean csvUsesSingleQuote = true;

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
        String sql = "SET TABLE " + tableName + " SOURCE \'" + csvPath + ";" + csvSettings + "' " + DESC;
        LOG.info("CSV import SQL: " + sql);
        executeDbCommand(sql, "Failed to import CSV: ");

        // Try to convert columns to numbers, where applicable.
        // "HyperSQL allows changing the type if all the existing values can be cast
        // into the new type without string truncation or loss of significant digits."
        if (true) {
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

}

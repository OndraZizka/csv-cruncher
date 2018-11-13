package cz.dynawest.csvcruncher;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public final class Cruncher
{
    private static final Logger LOG = Logger.getLogger(Cruncher.class.getName());

    public static final String TABLE_NAME__OUTPUT = "output";
    public static final long TIMESTAMP_SUBSTRACT = 1_530_000_000_000L; // To make the unique ID a smaller number.
    public static final String FILENAME_SUFFIX_CSV = ".csv";
    public static final Pattern REGEX_SQL_COLUMN_VALID_NAME = Pattern.compile("[a-z][a-z0-9_]*", Pattern.CASE_INSENSITIVE);
    public static final int MAX_STRING_COLUMN_LENGTH = 4092;

    private Connection jdbcConn;
    private HsqlDbHelper dbHelper;
    private final Options options;

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
            this.jdbcConn = DriverManager.getConnection("jdbc:hsqldb:file:" + dbPath + ";shutdown=true", "SA", "");
        }
        catch (IOException e) {
            throw new CsvCruncherException(String.format("Can't create HSQLDB data dir %s: %s", dbPath, e.getMessage()), e);
        }
        catch (SQLException e) {
            throw new CsvCruncherException(String.format("Can't connect to the database %s: %s", dbPath, e.getMessage()), e);
        }

        this.dbHelper = new HsqlDbHelper(this.jdbcConn);
    }

    /**
     * Performs the whole process.
     */
    public void crunch() throws Exception
    {
        this.options.validate();

        try
        {
            boolean addCounterColumn = options.initialRowNumber != null;
            boolean convertResultToJson = options.jsonExportFormat != Options.JsonExportFormat.NONE;
            boolean printAsArray = options.jsonExportFormat == Options.JsonExportFormat.ARRAY;

            Map<String, File> tablesToFiles = new HashMap<>();
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
                List<String> colNames = dbHelper.extractColumnsInfoFrom1LineSelect(this.options.sql);

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
                int rowsAffected = dbHelper.executeDbCommand(userSql, "Error executing user SQL: ");
                reachedStage = ReachedCrunchStage.OUTPUT_TABLE_FILLED;


                // Now let's convert it to JSON if necessary.
                if (convertResultToJson) {
                    Path destJsonFile = Paths.get(csvOutFile.toPath().toString() + ".json");
                    LOG.info(" * JSON output: " + destJsonFile);

                    try (Statement statement2 = this.jdbcConn.createStatement()) {
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
                dbHelper.executeDbCommand("DROP SCHEMA PUBLIC CASCADE", "Failed to delete the database: ");
                this.jdbcConn.close();
                LOG.info(" *** END SHUTDOWN CLEANUP SEQUENCE ***");
            }
        }
        catch (Exception ex) {
            throw ex; // Debug purposes
        }
    }

    private void cleanUpInputOutputTables(Map<String, File> inputTablesToFiles)
    {
        //if (reachedStage.passed(ReachedCrunchStage.INPUT_TABLES_CREATED))
        // I'm removing these stage checks, since the table might have been left
        // from previous run. Later let's implement a cleanup at start. TODO
        {
            dbHelper.detachTables(inputTablesToFiles.keySet(), "Could not delete the input table: ");
        }

        //if (reachedStage.passed(ReachedCrunchStage.OUTPUT_TABLE_CREATED))
        {
            dbHelper.detachTables(Collections.singleton(TABLE_NAME__OUTPUT), "Could not delete the output table: ");
        }

        //if (reachedStage.passed(ReachedCrunchStage.OUTPUT_TABLE_FILLED))
    }

    private long getInitialNumber()
    {
        long initialNumber;

        if (this.options.initialRowNumber != -1) {
            initialNumber = this.options.initialRowNumber;
        } else {
            // A timestamp at the beginning:
            //sql = "DECLARE crunchCounter BIGINT DEFAULT UNIX_MILLIS() - 1530000000000";
            //executeDbCommand(sql, "Failed creating the counter variable: ");
            // Uh oh. Variables can't be used in SELECTs.
            initialNumber = (System.currentTimeMillis() - TIMESTAMP_SUBSTRACT);
        }
        return initialNumber;
    }


    private static String normalizeFileNameForTableName(File fileName)
    {
        return fileName.getName().replaceFirst(".csv$", "").replaceAll("[^a-zA-Z0-9_]", "_");
    }


    private void createTableForInputFile(String tableName, File csvFileToBind, List<String> colNames, boolean ignoreFirst) throws SQLException
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
        dbHelper.executeDbCommand(sb.toString(), "Failed to CREATE TEXT TABLE: ");


        // Bind the table to the CSV file.
        String csvPath = csvFileToBind.getPath();
        csvPath = Utils.escapeSql(csvPath);
        String quoteCharacter = csvUsesSingleQuote ? "\\quote" : "\"";
        String ignoreFirstFlag = ignoreFirst ? "ignore_first=true;" : "";
        String csvSettings = "encoding=UTF-8;cache_rows=50000;cache_size=10240000;" + ignoreFirstFlag + "fs=,;qc=" + quoteCharacter;
        String DESC = readOnly ? "DESC" : "";  // Not a mistake, HSQLDB really has "DESC" here for read only.
        String sql = String.format("SET TABLE %s SOURCE '%s;%s' %s", tableName, csvPath, csvSettings, DESC);
        LOG.info("CSV import SQL: " + sql);
        dbHelper.executeDbCommand(sql, "Failed to import CSV: ");

        // SET TABLE <table name> SOURCE HEADER
        if (!input) {
            sql = String.format("SET TABLE %s SOURCE HEADER '%s'", tableName, sbCsvHeader.toString());
            LOG.info("CSV source header SQL: " + sql);
            dbHelper.executeDbCommand(sql, "Failed to set CSV header: ");
        }

        // Try to convert columns to numbers, where applicable.
        if (input) {
            dbHelper.optimizeTableCoumnsType(tableName, colNames);
        }
    }


    private class CounterColumn
    {
        String ddl = "";
        String value = "";

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
            dbHelper.executeDbCommand(sql, "Failed creating the counter sequence: ");

            sql = "ALTER SEQUENCE crunchCounter RESTART WITH " + initialNumber;
            dbHelper.executeDbCommand(sql, "Failed altering the counter sequence: ");

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

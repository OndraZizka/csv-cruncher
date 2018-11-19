package cz.dynawest.csvcruncher;

import cz.dynawest.csvcruncher.util.FilesUtils;
import cz.dynawest.csvcruncher.util.Utils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

@Slf4j
public final class Cruncher
{
    private static final Logger LOG = log;

    public static final String TABLE_NAME__OUTPUT = "output";
    public static final long TIMESTAMP_SUBSTRACT = 1_530_000_000_000L; // To make the unique ID a smaller number.
    public static final String FILENAME_SUFFIX_CSV = ".csv";
    public static final Pattern REGEX_SQL_COLUMN_VALID_NAME = Pattern.compile("[a-z][a-z0-9_]*", Pattern.CASE_INSENSITIVE);

    public static final String SQL_TABLE_PLACEHOLDER = "$table";
    private static final String DEFAULT_SQL = "SELECT "+ SQL_TABLE_PLACEHOLDER + " FROM " + SQL_TABLE_PLACEHOLDER;

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

        boolean addCounterColumn = options.initialRowNumber != null;
        boolean convertResultToJson = options.jsonExportFormat != Options.JsonExportFormat.NONE;
        boolean printAsArray = options.jsonExportFormat == Options.JsonExportFormat.ARRAY;

        Map<String, File> tablesToFiles = new HashMap<>();

        // Should the result have a unique incremental ID as an added 1st column?
        CounterColumn counterColumn = new CounterColumn();
        if (addCounterColumn)
            counterColumn.setDdlAndVal();


        try {
            // Sort the input paths.
            List<Path> inputPaths = this.options.inputPaths.stream().map(Paths::get).collect(Collectors.toList());
            inputPaths = FilesUtils.sortInputPaths(inputPaths, this.options.sortInputFiles);
            LOG.info(" --- Sorted input paths: --- " + inputPaths.stream().map(p -> "\n * " + p).reduce(String::concat).get());

            List<CruncherInputSubpart> inputSubparts;

            // Combine files. Should we concat the files or UNION the tables?
            if (this.options.combineInputFiles != Options.CombineInputFiles.NONE)
            {
                Map<Path, List<Path>> inputFileGroups = FilesUtils.expandFilterSortInputFilesGroups(inputPaths, options);

                ///Map<Path, List<Path>> resultingFilePathToConcatenatedFiles = FilesUtils.combineInputFiles(inputFileGroups, this.options);
                inputSubparts = FilesUtils.combineInputFiles(inputFileGroups, this.options);
                ///inputPaths = new ArrayList<>(inputSubparts.stream().map(x -> x.getCombinedFile()).collect(Collectors.toList()));
                ///LOG.info(" --- Combined input files: --- " + inputPaths.stream().map(p -> "\n * " + p).reduce(String::concat).orElse("NONE"));
                LOG.info(" --- Combined input files: --- " + inputSubparts.stream().map(p -> "\n * " + p.getCombinedFile()).reduce(String::concat).orElse("NONE"));
            }
            else {
                inputSubparts = inputPaths.stream().map(CruncherInputSubpart::trivial).collect(Collectors.toList());
                //inputPaths = new ArrayList<>(inputSubparts.stream().map(x -> x.getCombinedFile()).collect(Collectors.toList()));
            }

            if (inputSubparts.isEmpty())
                return;
            FilesUtils.validateInputFiles(inputSubparts);

            // For each input CSV file...
            for (CruncherInputSubpart inputSubpart : inputSubparts)
            {
                File csvInFile = Utils.resolvePathToUserDirIfRelative(inputSubpart.getCombinedFile());
                LOG.info(" * CSV input: " + csvInFile);

                String tableName = HsqlDbHelper.normalizeFileNameForTableName(csvInFile);
                File previousIfAny = tablesToFiles.put(tableName, csvInFile);
                if (previousIfAny != null)
                    throw new IllegalArgumentException("File names normalized to table names collide: " + previousIfAny + ", " + csvInFile);

                List<String> colNames = FilesUtils.parseColsFromFirstCsvLine(csvInFile);
                // Create a table and bind the CSV to it.
                dbHelper.createTableForInputFile(tableName, csvInFile, colNames, true, this.options.overwrite);

                inputSubpart.setTableName(tableName);
            }



            String genericSql = StringUtils.defaultString(this.options.sql, DEFAULT_SQL);
            List<CruncherOutputPart> outputs = new ArrayList<>();


            // SQL can be executed:
            // * for all tables, and generate a single result; if some table has changed, it would fail.
            if (!options.queryPerInputSubpart)
            {
                File csvOutFile = Utils.resolvePathToUserDirIfRelative(Paths.get(this.options.outputPathCsv));

                CruncherOutputPart output = new CruncherOutputPart(csvOutFile.toPath(), null);
                outputs.add(output);
            }
            // * per input, and generate one result per execution.
            else {
                Set<Path> usedOutputFiles = new HashSet<Path>();

                for (CruncherInputSubpart inputSubpart : inputSubparts)
                {
                    Path outputFile = Paths.get(options.getOutputPathCsv()).resolve(inputSubpart.getCombinedFile().getFileName());
                    outputFile = FilesUtils.getNonUsedName(outputFile, usedOutputFiles);

                    CruncherOutputPart output = new CruncherOutputPart(outputFile, inputSubpart.getTableName());
                    outputs.add(output);
                }
            }


            // TODO: For each output...
            for (CruncherOutputPart output : outputs)
            {
                File csvOutFile = output.getOutputFile().toFile();
                String sql = genericSql;
                String outputTableName = TABLE_NAME__OUTPUT;
                if (output.getInputTableName() != null) {
                    sql = sql.replace(SQL_TABLE_PLACEHOLDER, output.getInputTableName());
                    outputTableName = output.getInputTableName() + "_out";
                }


                // Create the parent dir.
                File dirToCreate = csvOutFile.getAbsoluteFile().getParentFile();
                dirToCreate.mkdirs();

                // Get the columns info: Perform the SQL, LIMIT 1.
                List<String> colNames = dbHelper.extractColumnsInfoFrom1LineSelect(sql);
                output.setColumnNames(colNames);


                // Write the result into a CSV
                LOG.info(" * CSV output: " + csvOutFile);
                dbHelper.createTableAndBindCsv(outputTableName, csvOutFile, colNames, true, counterColumn.ddl, false, this.options.overwrite);

                // The provided SQL could be something like "SELECT @counter, foo, bar FROM ..."
                //String selectSql = this.options.sql.replace("@counter", value);
                // On the other hand, that's too much space for the user to screw up. Let's force it:
                String selectSql = sql.replace("SELECT ", "SELECT " + counterColumn.value + " ");
                output.setSql(selectSql);

                String userSql = "INSERT INTO " + outputTableName + " (" + selectSql + ")";
                LOG.info(" * User's SQL: " + userSql);
                //LOG.info("\n  Tables and column types:\n" + this.formatListOfAvailableTables(true));///
                int rowsAffected = dbHelper.executeDbCommand(userSql, "Error executing user SQL: ");


                // Now let's convert it to JSON if necessary.
                if (convertResultToJson) {
                    Path destJsonFile = Paths.get(csvOutFile.toPath().toString() + ".json");
                    LOG.info(" * JSON output: " + destJsonFile);

                    try (Statement statement2 = this.jdbcConn.createStatement()) {
                        FilesUtils.convertResultToJson(
                                statement2.executeQuery("SELECT * FROM " + outputTableName),
                                destJsonFile,
                                printAsArray
                        );
                        if (!this.options.keepWorkFiles)
                            csvOutFile.deleteOnExit();
                    }
                }
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

    /**
     * @return The initial number to use for unique row IDs.
     *         Takes the value from options, or generates from timestamp if not set.
     */
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


    /**
     * Information for the extra column used to add a unique id to each row.
     */
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

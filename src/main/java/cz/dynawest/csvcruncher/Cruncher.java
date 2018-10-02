package cz.dynawest.csvcruncher;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;

public class Cruncher
{
    private static final Logger log = Logger.getLogger(App.class.getName());

    public static final String TABLE_NAME__OUTPUT = "output";
    public static final long TIMESTAMP_SUBSTRACT = 1_530_000_000_000L; // To make the unique ID a smaller number.
    public static final String FILENAME_SUFFIX_CSV = ".csv";

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

            byte reachedStage = 0;
            boolean crunchSuccess = false;

            File csvOutFile = Utils.resolvePathToUserDirIfRelative(Paths.get(this.options.outputPathCsv));
            csvOutFile.getAbsoluteFile().getParentFile().mkdirs();

            try
            {
                // Sort
                List<Path> inputPaths = this.options.inputPaths.stream().map(Paths::get).collect(Collectors.toList());
                inputPaths = sortInputPaths(inputPaths, this.options.sortInputFiles);

                log.info(" --- Sorted input paths: --- ");
                for (Path path : inputPaths) {
                    log.info(" * " + path);
                }


                // Combine files. Should we concat the files or UNION the tables?
                if (this.options.combineInputFiles != Options.CombineInputFiles.NONE) {
                    List<Path> concatenatedFiles = combineInputFiles(inputPaths, this.options);
                    inputPaths = concatenatedFiles;

                    log.info(" --- Combined input files: --- ");
                    for (Path path : inputPaths) {
                        log.info(" * " + path);
                    }
                }

                log.info(" --- ===================== --- ");

                // For each input CSV file...
                for (Path path : inputPaths) {
                    File csvInFile = Utils.resolvePathToUserDirIfRelative(path);
                    log.info(" * CSV input: " + csvInFile);

                    String tableName = normalizeFileNameForTableName(csvInFile);
                    File previousIfAny = tablesToFiles.put(tableName, csvInFile);
                    if (previousIfAny != null)
                        throw new IllegalArgumentException("File names normalized to table names collide: " + previousIfAny + ", " + csvInFile);

                    String[] colNames = parseColsFromFirstLine(csvInFile);
                    // Read the CSV into a table.
                    this.createTableForCsvFile(tableName, csvInFile, colNames, true);
                }


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


                // Perform the SQL
                PreparedStatement statement;
                try {
                    statement = this.conn.prepareStatement(this.options.sql + " LIMIT 1");
                }
                catch (SQLSyntaxErrorException ex) {
                    if (ex.getMessage().contains("object not found:")) {
                        String tableNames = formatListOfAvailableTables(); // TODO
                        throw new RuntimeException(
                                "Looks like you are referring to a table that was not created.\n"
                                + "This could mean that you have a typo in the input file name,\n"
                                + "or maybe you use --combineInputs but try to use the original inputs.\n"
                                + "These tables are actually available:\n"
                                + tableNames + "\n\n Message from the database:\n  "
                                + ex.getMessage(), ex);
                    }
                    throw new RuntimeException("Seems your SQL contains errors:\n" + ex.getMessage(), ex);
                }
                ResultSet rs = statement.executeQuery();

                // Column names
                String [] colNames = new String[rs.getMetaData().getColumnCount()];
                for (int col = 0; col < colNames.length; col++) {
                    colNames[col] = rs.getMetaData().getColumnName(col + 1).toLowerCase();
                }

                // Write the result into a CSV
                log.info(" * CSV output: " + csvOutFile);
                this.createTableForCsvFile(TABLE_NAME__OUTPUT, csvOutFile, colNames, true, counterColumnDdl, false);
                reachedStage = 2;

                // The provided SQL could be something like "SELECT @counter, foo, bar FROM ..."
                //String selectSql = this.options.sql.replace("@counter", counterColumnVal);
                // On the other hand, that's too much space for the user to screw up. Let's force it:
                String selectSql = this.options.sql.replace("SELECT ", "SELECT " + counterColumnVal + " ");

                String userSql = "INSERT INTO output (" + selectSql + ")";
                log.info(" * User's SQL: " + userSql);
                statement = this.conn.prepareStatement(userSql);
                int rowsAffected = statement.executeUpdate();
                crunchSuccess = true;


                // Now let's convert it to JSON if necessary.
                reachedStage = 3;

                if (convertResultToJson) {
                    Path destJsonFile = Paths.get(csvOutFile.toPath().toString() + ".json");
                    log.info(" * JSON output: " + destJsonFile);

                    try (Statement statement2 = this.conn.createStatement()) {
                        convertResultToJson(
                                statement2.executeQuery("SELECT * FROM " + TABLE_NAME__OUTPUT),
                                destJsonFile,
                                printAsArray
                        );
                    }
                }
            }
            finally
            {
                if (crunchSuccess)
                {
                    for (Map.Entry<String, File> tableAndFile: tablesToFiles.entrySet()) {
                        //if (reachedStage >= 1)
                        this.detachTable(tableAndFile.getKey(), false);
                    }

                    if (reachedStage >= 2)
                        this.detachTable("output", false);

                    PreparedStatement ps = this.conn.prepareStatement("DROP SCHEMA PUBLIC CASCADE");
                    ps.execute();
                    this.conn.close();
                }
            }
        }
        catch (Exception ex)
        {
            throw ex;
        }
    }

    private String formatListOfAvailableTables()
    {
        StringBuilder sb = new StringBuilder();

        String sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'PUBLIC'";
        try (ResultSet rs = this.conn.createStatement().executeQuery(sql)) {
            while(rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                sb.append(" * ").append(tableName).append('\n');
            }
            if (sb.length() == 0)
                return "    (No tables)";
            return sb.toString();
        }
        catch (SQLException ex) {
            String msg = "Failed listing tables: " + ex.getMessage();
            log.severe(msg);
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
     * Combine the input files (typically, concatenate).
     * If the paths are directories, they may be combined per each directory, per input dir, per input subdir, or all into one.
     * The combined input files will be witten under the respective "group root directory".
     * For COMBINE_ALL_FILES, the combined file will be written under current user directory ("user.dir").
     */
    private static List<Path> combineInputFiles(List<Path> inputPaths, Options options) throws IOException
    {
        switch (options.combineInputFiles) {
            case NONE: default: return inputPaths;
            case INTERSECT: case EXCEPT: throw new UnsupportedOperationException("INTERSECT and EXCEPT combining is not implemented yet.");
            case CONCAT:
                log.info("Concatenating input files:");

                // First, expand the directories.
                Map<Path, List<Path>> fileGroupsToConcat = new HashMap<>();
                // null will be used as a special key for COMBINE_ALL_FILES.
                fileGroupsToConcat.putIfAbsent(null, new ArrayList<>());

                List<Path> concatenatedFiles = new ArrayList<>();
                for (Path inputPath: inputPaths) {
                    log.info(" * About to concat " + inputPath);
                    try {
                        // Put files simply to "global" group. Might be improved in the future.
                        if (inputPath.toFile().isFile())
                            fileGroupsToConcat.get(null).add(inputPath);
                        // Walk directories for CSV, and group them as per options.combineDirs.
                        if (inputPath.toFile().isDirectory()) {

                            Consumer<Path> fileToGroupSorter = null;
                            switch (options.combineDirs) {
                                case COMBINE_ALL_FILES: {
                                    List<Path> fileGroup = fileGroupsToConcat.get(null);
                                    fileToGroupSorter = curPath -> { fileGroup.add(curPath); };
                                    } break;
                                case COMBINE_PER_INPUT_DIR: {
                                    List<Path> fileGroup = fileGroupsToConcat.get(inputPath);
                                    fileToGroupSorter = curPath -> { fileGroup.add(curPath); };
                                    } break;
                                case COMBINE_PER_EACH_DIR: {
                                    //List<Path> fileGroup = fileGroupsToConcat.get(inputPath);
                                    fileToGroupSorter = curPath -> { fileGroupsToConcat.get(curPath.toAbsolutePath()).add(curPath); };
                                }
                            }

                            log.info("   *** About to walk" + inputPath);
                            Files.walk(inputPath)
                                    .filter(curPath -> Files.isRegularFile(curPath) && curPath.getFileName().toString().endsWith(FILENAME_SUFFIX_CSV))
                                    .forEach(fileToGroupSorter);
                            log.info("   *** After walking: " + fileGroupsToConcat.size());
                        }
                    } catch (Exception ex) {
                        throw new RuntimeException("Failed combining the input files in ");
                    }
                }

                // Then combine the file sets.

                Path defaultDestDir = Paths.get(options.outputPathCsv).getParent(); // Paths.get(System.getProperty("user.dir"));
                int filesCounter = 0;

                for (Map.Entry<Path, List<Path>> fileGroup : fileGroupsToConcat.entrySet()) {
                    log.info("   *** Combining " + fileGroup.getKey() + " : " + fileGroup.getValue());

                    // Sort
                    List<Path> sortedPaths = sortInputPaths(fileGroup.getValue(), options.sortInputFiles).stream().collect(Collectors.toList());

                    Path destDir = fileGroup.getKey();
                    fileGroupsToConcat.put(destDir, sortedPaths);

                    // Assorted files will be put to the "current dir". Or to the result dir?
                    if (destDir == null) {
                        destDir = defaultDestDir.resolve("concat");
                        Files.createDirectories(destDir);
                    }

                    // Come up with some good name for the combined file.
                    Path concatenatedFilePath =
                        destDir.resolve(destDir.getFileName().toString()
                            + "_" + ++filesCounter + FILENAME_SUFFIX_CSV);
                    // TODO: Optionally this should go to to defaultDestDir.
                    //       1) Find common deepest ancestor dir.
                    //       2) From each dest path, substract the differentiating subpath.
                    //       3) Create the subdirs in defaultDestDir and save there.

                    concatFiles(sortedPaths, concatenatedFilePath);
                    concatenatedFiles.add(concatenatedFilePath);
                }

                return concatenatedFiles;
        }
    }

    /**
     * Concatenates given files into a file in the resultPath, named "CsvCruncherConcat.csv".
     * If some of the input files does not end with a new line, it is appended after that file.
     * @return The path to the created file.
     */
    private static Path concatFiles(List<Path> filesToConcat, Path resultPath)
    {
        File resultFile = resultPath.toFile();
        try(FileOutputStream resultOS = new FileOutputStream(resultFile);) {
            for (Path pathToConcat : filesToConcat) {
                try (FileInputStream fileToConcatIS = new FileInputStream(pathToConcat.toFile())) {
                    IOUtils.copy(fileToConcatIS, resultOS);
                }
                // Read the last byte, check if it's a \n. If not, let's append one.
                try (FileInputStream fileToConcatIS = new FileInputStream(pathToConcat.toFile())) {
                    fileToConcatIS.skip(pathToConcat.toFile().length()-1);
                    if ('\n' != fileToConcatIS.read())
                        resultOS.write('\n');
                }
            }
        }
        catch (Exception ex) {
            throw new RuntimeException("Failed concatenating files into " + resultPath + ": " + ex.getMessage(), ex);
        }
        return  resultFile.toPath();
    }

    private static List<Path> sortInputPaths(List<Path> inputPaths, Options.SortInputFiles sortMethod)
    {
        switch (sortMethod) {
            case PARAMS_ORDER: return inputPaths;
            case ALPHA: inputPaths = new ArrayList<>(inputPaths); Collections.sort(inputPaths); return inputPaths;
            case TIME: throw new UnsupportedOperationException("Sorting by time not implemented yet.");
            default: throw new UnsupportedOperationException("Unkown sorting method.");
        }
    }

    /**
     * Writes the given resultset to a JSON file at given path, one entry per line, optionally as an JSON array.
     */
    private void convertResultToJson(ResultSet resultSet, Path destFile, boolean printAsArray)
    {
        try (
                OutputStream outS = new BufferedOutputStream(new FileOutputStream(destFile.toFile()));
                Writer outW = new OutputStreamWriter(outS, StandardCharsets.UTF_8);
        ) {
            ResultSetMetaData metaData = resultSet.getMetaData();

            // Cache which cols are numbers.

            boolean[] colsAreNumbers;
            cacheWhichColumnsNeedJsonQuotes(metaData);


            if (printAsArray)
                outW.append("[\n");

            while (resultSet.next()) {
                // javax.json way
                JsonObjectBuilder builder = Json.createObjectBuilder();
                // Columns
                for (int colIndex = 1; colIndex <= metaData.getColumnCount(); colIndex++) {
                    addTheRightTypeToJavaxJsonBuilder(resultSet, colIndex, builder);
                }
                JsonObject jsonObject = builder.build();
                JsonWriter writer = Json.createWriter(outW);
                writer.writeObject(jsonObject);


                /*// Hand-made
                outW.append("{\"");
                // Columns
                for (int colIndex = 1; colIndex <= metaData.getColumnCount(); colIndex++) {
                    // Key
                    outW.append(org.json.simple.JSONObject.escape(metaData.getColumnLabel(colIndex) ));
                    outW.append("\":");
                    // TODO
                    String val = formatValueForJson(resultSet, colIndex, colsAreNumbers);
                    if (null == val) {
                        outW.append("null");
                        continue;
                    }
                    if (!colsAreNumbers[colIndex])
                        outW.append('"');
                    outW.append(val);
                    if (!colsAreNumbers[colIndex])
                        outW.append('"');
                }
                outW.append("\"}");
                /**/

                outW.append(printAsArray ? ",\n" : "\n");
            }
            if (printAsArray)
                outW.append("]\n");
        }
        catch (Exception ex) {
            throw new RuntimeException("Failed browsing the final query results: " + ex.getMessage(), ex);
        }
    }

    private void executeDbCommand(String sql, String errorMsg) throws SQLException
    {
        try {
            this.conn.createStatement().execute(sql);
        } catch (Exception ex) {
            throw new RuntimeException(errorMsg + sql + "\n" + this.conn.getWarnings().getNextWarning());
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

    private static String[] parseColsFromFirstLine(File file) throws IOException
    {
        Pattern pat = Pattern.compile("[a-z][a-z0-9]*", 2);
        Matcher mat = pat.matcher("");
        ArrayList cols = new ArrayList();
        LineIterator lineIterator = FileUtils.lineIterator(file);
        if (!lineIterator.hasNext())
        {
            throw new IllegalStateException("No first line with columns definition (format: [# ] <colName> [, ...]) in: " + file.getPath());
        }
        else
        {
            String line = lineIterator.nextLine();
            line = StringUtils.stripStart(line, "#");
            String[] colNames = StringUtils.splitPreserveAllTokens(line, ",;");

            for (int i = 0; i < colNames.length; ++i)
            {
                String colName = colNames[i];
                colName = colName.trim();
                if (0 == colName.length())
                    throw new IllegalStateException("Empty column name (separators: ,; ) in: " + file.getPath());

                if (!mat.reset(colName).matches())
                    throw new IllegalStateException("Colname must be valid SQL identifier, i.e. must match /[a-z][a-z0-9]*/i in: " + file.getPath());

                cols.add(colName);
            }

            return (String[]) ((String[]) cols.toArray(new String[cols.size()]));
        }
    }

    private void createTableForCsvFile(String tableName, File csvFileToBind, String[] colNames, boolean ignoreFirst) throws SQLException, FileNotFoundException
    {
        createTableForCsvFile(tableName, csvFileToBind, colNames, ignoreFirst, "", true);
    }


    private void createTableForCsvFile(String tableName, File csvFileToBind, String[] colNames, boolean ignoreFirst, String counterColumnDdl, boolean readOnlyX) throws SQLException, FileNotFoundException
    {
        boolean readOnly = false;
        boolean csvUsesSingleQuote = true;

        StringBuilder sbCsvHeader = new StringBuilder("# ");
        StringBuilder sb = (new StringBuilder("CREATE TEXT TABLE ")).append(tableName).append(" ( ");

        // The counter column, if any.
        sb.append(counterColumnDdl);

        int colCount = colNames.length;

        // Columns
        String colName;
        for (int colIndex = 0; colIndex < colCount; ++colIndex)
        {
            colName = colNames[colIndex];
            sbCsvHeader.append(colName).append(", ");
            colName = Utils.escapeSql(colName);
            sb.append(colName).append(" VARCHAR(255), ");
        }
        sbCsvHeader.delete(sbCsvHeader.length() - 2, sbCsvHeader.length());
        sb.delete(sb.length() - 2, sb.length());
        sb.append(" )");
        log.info("Table DDL SQL: " + sb.toString());

        PreparedStatement statement = this.conn.prepareStatement(sb.toString());
        boolean success = statement.execute();

        // Bind the table to the CSV file.
        String csvPath = csvFileToBind.getPath();
        csvPath = Utils.escapeSql(csvPath);
        String quoteCharacter = csvUsesSingleQuote ? "\\quote" : "\"";
        String ignoreFirstFlag = ignoreFirst ? "ignore_first=true;" : "";
        String csvSettings = "encoding=UTF-8;cache_rows=50000;cache_size=10240000;" + ignoreFirstFlag + "fs=,;qc=" + quoteCharacter;
        String DESC = readOnly ? "DESC" : "";  // Not a mistake, HSQLDB really has "DESC" here for read only.
        String sql = "SET TABLE " + tableName + " SOURCE \'" + csvPath + ";" + csvSettings + "' " + DESC;
        log.fine("CSV import SQL: " + sql);
        statement = this.conn.prepareStatement(sql);
        success = statement.execute();
    }

    private void detachTable(String name, boolean reattach) throws SQLException
    {
        String sql = "SET TABLE " + Utils.escapeSql(name) + " SOURCE " + (reattach ? "ON" : "OFF");
        PreparedStatement ps = this.conn.prepareStatement(sql);
        boolean succ = ps.execute();
    }



    private boolean[] cacheWhichColumnsNeedJsonQuotes(ResultSetMetaData metaData) throws SQLException
    {
        boolean[] colsAreNumbers = new boolean[metaData.getColumnCount()+1];
        int colType;
        for (int colIndex = 1; colIndex <= metaData.getColumnCount(); colIndex++ ) {
            colType = metaData.getColumnType(colIndex);
            colsAreNumbers[colIndex] =
                    colType == Types.TINYINT || colType == Types.SMALLINT || colType == Types.INTEGER || colType == Types.BIGINT
                            || colType == Types.DECIMAL || colType == Types.NUMERIC ||  colType == Types.BIT || colType == Types.ROWID || colType == Types.DOUBLE || colType == Types.FLOAT || colType == Types.BOOLEAN;
        }
        return colsAreNumbers;
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
                log.severe("Unsupported type of column " + metaData.getColumnLabel(colIndex) + ": " + metaData.getColumnTypeName(colIndex));
                return null;
        }
        if (resultSet.wasNull())
            return null;
        return val;
    }

    /**
     * Used in case we use javax.json.JsonBuilder.
     * This also needs JsonProviderImpl.
     */
    private static void addTheRightTypeToJavaxJsonBuilder(ResultSet resultSet, int colIndex, JsonObjectBuilder builder) throws SQLException
    {
        ResultSetMetaData metaData = resultSet.getMetaData();
        String columnLabel = metaData.getColumnLabel(colIndex);
        if (columnLabel.matches("[A-Z]+"))
            columnLabel = columnLabel.toLowerCase();

        switch (metaData.getColumnType(colIndex)) {
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.CLOB:
                builder.add(columnLabel, resultSet.getString(colIndex)); break;
            case Types.TINYINT:
            case Types.BIT:
                builder.add(columnLabel, resultSet.getByte(colIndex)); break;
            case Types.SMALLINT: builder.add(columnLabel, resultSet.getShort(colIndex)); break;
            case Types.INTEGER:  builder.add(columnLabel, resultSet.getInt(colIndex)); break;
            case Types.BIGINT:   builder.add(columnLabel, resultSet.getLong(colIndex)); break;
            case Types.BOOLEAN:  builder.add(columnLabel, resultSet.getBoolean(colIndex)); break;
            case Types.FLOAT:    builder.add(columnLabel, resultSet.getFloat(colIndex)); break;
            case Types.DOUBLE:
            case Types.DECIMAL:  builder.add(columnLabel, resultSet.getDouble(colIndex)); break;
            case Types.NUMERIC:  builder.add(columnLabel, resultSet.getBigDecimal(colIndex)); break;
            case Types.DATE:    builder.add(columnLabel, ""+resultSet.getDate(colIndex)); break;
            case Types.TIME:    builder.add(columnLabel, ""+resultSet.getTime(colIndex)); break;
            case Types.TIMESTAMP:    builder.add(columnLabel, (""+resultSet.getTimestamp(colIndex)).replace(' ', 'T')); break; // JS Date() takes "1995-12-17T03:24:00"
        }
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

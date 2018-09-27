package cz.dynawest.csvcruncher;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;

public class Cruncher
{
    private static final Logger log = Logger.getLogger(App.class.getName());
    private Connection conn;
    private Cruncher.Options options;

    public Cruncher(Cruncher.Options options) throws ClassNotFoundException, SQLException
    {
        this.options = options;
        this.init();
    }

    private void init() throws ClassNotFoundException, SQLException
    {
        System.setProperty("textdb.allow_full_path", "true");
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
            byte reachedStage = 0;
            boolean success = false;

            File outFile = this.getFileObject(this.options.csvPathOut);
            outFile.getAbsoluteFile().getParentFile().mkdirs();

            try
            {
                // For each input CSV file...
                for (String path : this.options.csvPathIn) {
                    File csvInFile = this.getFileObject(path);
                    log.info(" * CSV input: " + csvInFile);

                    String tableName = normalizeFileNameForTableName(csvInFile);
                    File previousIfAny = tablesToFiles.put(tableName, csvInFile);
                    if (previousIfAny != null)
                        throw new IllegalArgumentException("File names normalized to table names collide: " + previousIfAny + ", " + csvInFile);

                    String[] colNames = parseColsFromFirstLine(csvInFile);
                    // Read the CSV into a table.
                    this.createTableForCsvFile(tableName, csvInFile, colNames, true);
                }

                // Perform the SQL
                PreparedStatement statement = this.conn.prepareStatement(this.options.sql);
                ResultSet rs = statement.executeQuery();

                // Column names
                String [] colNames = new String[rs.getMetaData().getColumnCount()];
                for (int col = 0; col < colNames.length; col++) {
                    colNames[col] = rs.getMetaData().getColumnName(col + 1);
                }

                // Write the result into a CSV
                this.createTableForCsvFile("output", outFile, colNames, true);
                reachedStage = 2;
                String userSql = "INSERT INTO output (" + this.options.sql + ")";
                log.info("User\'s SQL: " + userSql);
                statement = this.conn.prepareStatement(userSql);
                int rowsAffected = statement.executeUpdate();
                success = true;
            }
            finally
            {
                if (success)
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

    private String normalizeFileNameForTableName(File fileName)
    {
        return fileName.getName().replaceFirst(".csv$", "").replaceAll("[^a-zA-Z0-9_]", "_");
    }

    private void validateParameters() throws FileNotFoundException
    {
        if (this.options.csvPathIn == null || this.options.csvPathIn.isEmpty())
            throw new IllegalArgumentException(" -in is not set.");

        if (this.options.sql == null)
            throw new IllegalArgumentException(" -sql is not set.");

        if (this.options.csvPathOut == null)
            throw new IllegalArgumentException(" -out is not set.");


        for (String path : this.options.csvPathIn) {
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

    private void createTableForCsvFile(String tableName, File csvFileIn, String[] colNames, boolean ignoreFirst) throws SQLException, FileNotFoundException
    {
        boolean readOnly = false;
        StringBuilder sbCsvHeader = new StringBuilder("# ");
        StringBuilder sb = (new StringBuilder("CREATE TEXT TABLE ")).append(tableName).append(" ( ");
        int succ = colNames.length;

        String colName;
        for (int csvPathIn = 0; csvPathIn < succ; ++csvPathIn)
        {
            colName = colNames[csvPathIn];
            sbCsvHeader.append(colName).append(", ");
            colName = escapeSql(colName);
            sb.append(colName).append(" VARCHAR(255), ");
        }

        sb.delete(sb.length() - 2, sb.length());
        sb.append(" )");
        sbCsvHeader.delete(sbCsvHeader.length() - 2, sbCsvHeader.length());
        log.info("SQL: " + sb.toString());

        PreparedStatement statement = this.conn.prepareStatement(sb.toString());
        boolean success = statement.execute();

        String csvPath = csvFileIn.getPath();
        csvPath = escapeSql(csvPath);
        String ignoreFirstFlag = ignoreFirst ? "ignore_first=true;" : "";
        String DESC = readOnly ? "DESC" : "";
        statement = this.conn.prepareStatement("SET TABLE " + tableName + " SOURCE \'" + csvPath + ";" + ignoreFirstFlag + "fs=,\' " + DESC);
        success = statement.execute();
    }

    private void detachTable(String name, boolean reattach) throws SQLException
    {
        String sql = "SET TABLE " + escapeSql(name) + " SOURCE " + (reattach ? "ON" : "OFF");
        PreparedStatement ps = this.conn.prepareStatement(sql);
        boolean succ = ps.execute();
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

    private File getFileObject(String path)
    {
        return Paths.get(path).isAbsolute() ? new File(path) : new File(System.getProperty("user.dir"), path);
    }

    private String escapeSql(String str)
    {
        return str.replace("'", "''");
    }

    protected static class Options
    {
        protected List<String> csvPathIn = new ArrayList<>();
        protected String sql;
        protected String csvPathOut;
        protected String dbPath = null;

        public boolean isFilled()
        {
            return this.csvPathIn != null && this.csvPathOut != null && this.sql != null;
        }

        public String toString()
        {
            return "\n    dbPath: " + this.dbPath + "\n    csvPathIn: " + this.csvPathIn + "\n    csvPathOut: " + this.csvPathOut + "\n    sql: " + this.sql;
        }
    }
}

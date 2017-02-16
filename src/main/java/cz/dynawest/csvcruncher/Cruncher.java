package cz.dynawest.csvcruncher;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
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

    public void crunch() throws Exception
    {
        try
        {
            if (this.options.csvPathIn == null)
                throw new IllegalArgumentException(" -in is not set.");

            if (this.options.sql == null)
                throw new IllegalArgumentException(" -sql is not set.");

            if (this.options.csvPathOut == null)
                throw new IllegalArgumentException(" -out is not set.");

            File ex = new File(this.options.csvPathIn);
            if (!ex.exists())
                throw new FileNotFoundException(ex.getPath());

            byte reachedStage = 0;
            boolean success = true;

            try
            {
                File ps = this.getFileObject(this.options.csvPathIn);
                File outFile = this.getFileObject(this.options.csvPathOut);
                log.info(this.options.toString());
                log.info("******* inPath: " + this.options.csvPathIn);
                log.info("******* inFile: " + ps.getPath());
                if (!ps.exists())
                {
                    throw new FileNotFoundException("CSV file not found: " + ps.getPath());
                }

                outFile.getAbsoluteFile().getParentFile().mkdirs();
                String[] colNames = parseColsFromFirstLine(ex);
                this.createTableForCsvFile("indata", ps, colNames, true);
                PreparedStatement ps1 = this.conn.prepareStatement(this.options.sql);
                ResultSet rs = ps1.executeQuery();
                colNames = new String[rs.getMetaData().getColumnCount()];
                int sql = 0;

                while (true)
                {
                    if (sql >= colNames.length)
                    {
                        this.createTableForCsvFile("output", outFile, colNames, true);
                        reachedStage = 2;
                        String userSql = "INSERT INTO output (" + this.options.sql + ")";
                        log.info("User\'s SQL: " + userSql);
                        ps1 = this.conn.prepareStatement(userSql);
                        int rowsAffected = ps1.executeUpdate();
                        success = false;
                        break;
                    }

                    colNames[sql] = rs.getMetaData().getColumnName(sql + 1);
                    ++sql;
                }
            }
            finally
            {
                if (success)
                {
                    if (reachedStage >= 1)
                        this.detachTable("indata", false);

                    if (reachedStage >= 2)
                        this.detachTable("output", false);

                    PreparedStatement ps = this.conn.prepareStatement("DROP SCHEMA PUBLIC CASCADE");
                    ps.execute();
                    this.conn.close();
                }
            }

            if (reachedStage >= 1)
                this.detachTable("indata", false);

            if (reachedStage >= 2)
                this.detachTable("output", false);

            PreparedStatement statement = this.conn.prepareStatement("DROP SCHEMA PUBLIC CASCADE");
            statement.execute();
            this.conn.close();
        }
        catch (Exception ex)
        {
            throw ex;
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
        return !path.isEmpty() && path.charAt(0) == 47 ? new File(path) : new File(System.getProperty("user.dir"), path);
    }

    private String escapeSql(String str)
    {
        return str.replace("'", "''");
    }

    protected static class Options
    {
        protected String csvPathIn;
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

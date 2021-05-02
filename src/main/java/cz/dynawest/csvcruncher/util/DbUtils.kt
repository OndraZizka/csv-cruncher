package cz.dynawest.csvcruncher.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public final class DbUtils
{
    public static List<String> getResultSetColumnNames(ResultSet rs) throws SQLException
    {
        String [] colNames_ = new String[rs.getMetaData().getColumnCount()];
        for (int colIndex = 0; colIndex < colNames_.length; colIndex++) {
            colNames_[colIndex] = rs.getMetaData().getColumnName(colIndex + 1).toLowerCase();
        }
        return Arrays.asList(colNames_);
    }

    public static Map<String, String> getResultSetColumnNamesAndTypes(ResultSet rs) throws SQLException
    {
        int columnCount = rs.getMetaData().getColumnCount();
        Map<String,String> columns = new LinkedHashMap<>(columnCount);

        for (int colIndex = 0; colIndex < columnCount; colIndex++) {
            columns.put(rs.getMetaData().getColumnName(colIndex + 1), rs.getMetaData().getColumnTypeName(colIndex + 1));
        }
        return columns;
    }

    /**
     * Tells apart whether the "object not found" was a column or a table.
     * Relies on HSQLDB's exception message, which looks like this:
     *  USER LACKS PRIVILEGE OR OBJECT NOT FOUND: JOBNAME IN STATEMENT [SELECT JOBNAME, FROM
     *
     *     user lacks privilege or object not found: JOBNAME in statement [SELECT jobName, ... FROM ...]
     *
     * @return true if column, false if table (or something else).
     */
    public static boolean analyzeWhatWasNotFound(String message)
    {
        String notFoundName = StringUtils.substringAfter(message, "object not found: ");
        notFoundName = StringUtils.substringBefore(notFoundName, " in statement [");

        message = message.toUpperCase().replace('\n', ' ');

        //String sqlRegex = "[^']*\\[SELECT .*" + notFoundName + ".*FROM.*";
        String sqlRegex = ".*SELECT.*"+notFoundName+".*FROM.*";
        //LOG.finer(String.format("\n\tNot found object: %s\n\tMsg: %s\n\tRegex: %s", notFoundName, message.toUpperCase(), sqlRegex));

        return message.toUpperCase().matches(sqlRegex);
    }


    /**
     * Dump the content of a table. Debug code.
     */
    public static void testDumpSelect(String sql, Connection jdbcConn) throws SQLException
    {

        sql = sql.startsWith("SELECT ") ? sql : "SELECT * FROM " + sql;

        Statement ps = jdbcConn.createStatement();
        ResultSet rs = ps.executeQuery(sql);
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

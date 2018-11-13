package cz.dynawest.csvcruncher;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;

public class DbUtils
{
    private static final Logger LOG = Logger.getLogger(DbUtils.class.getName());

    static List<String> getResultSetColumnNames(ResultSet rs) throws SQLException
    {
        String [] colNames_ = new String[rs.getMetaData().getColumnCount()];
        for (int colIndex = 0; colIndex < colNames_.length; colIndex++) {
            colNames_[colIndex] = rs.getMetaData().getColumnName(colIndex + 1).toLowerCase();
        }
        return Arrays.asList(colNames_);
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
    static boolean analyzeWhatWasNotFound(String message, String sql)
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
     * Prepares a list of tables in the given JDBC connections, in the PUBLIC schema.
     */
    static String formatListOfAvailableTables(boolean withColumns, Connection jdbcConn)
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
}

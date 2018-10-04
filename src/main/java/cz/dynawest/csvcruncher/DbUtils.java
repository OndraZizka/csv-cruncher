package cz.dynawest.csvcruncher;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class DbUtils
{
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
}

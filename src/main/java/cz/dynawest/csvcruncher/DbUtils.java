package cz.dynawest.csvcruncher;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

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
}

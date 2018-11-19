package cz.dynawest.csvcruncher;

import java.nio.file.Path;
import java.util.List;
import lombok.Data;
import lombok.NonNull;

/**
 * Info about one file to be created by one SQL query.
 */
@Data
public class CruncherOutputPart
{
    public static final String OUTPUT_TABLE_SUFFIX = "_out";

    @NonNull private Path outputFile;

    private final String inputTableName;


    // These are filled during processing.

    private String sql;

    private List<String> columnNames;

    public String deriveOutputTableName()
    {
        if (getInputTableName() == null)
            return Cruncher.TABLE_NAME__OUTPUT;
        else
            return getInputTableName() + OUTPUT_TABLE_SUFFIX;
    }
}

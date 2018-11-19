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
    @NonNull private Path outputFile;

    @NonNull private String inputTableName;


    // These are filled during processing.

    private String sql;

    private List<String> columnNames;
}

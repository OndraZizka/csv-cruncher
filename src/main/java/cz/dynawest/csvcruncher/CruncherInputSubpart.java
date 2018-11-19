package cz.dynawest.csvcruncher;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import lombok.Data;

/**
 * One part of input data, maps to one or more SQL tables. Can be created out of multiple input files.
 */
@Data
public class CruncherInputSubpart
{
    private Path originalInputPath;
    private Path combinedFile;
    private List<Path> combinedFromFiles;
    private String tableName;

    public static CruncherInputSubpart trivial(Path path)
    {
        CruncherInputSubpart cis = new CruncherInputSubpart();
        cis.setOriginalInputPath(path);
        cis.setCombinedFile(path);
        cis.setCombinedFromFiles(Collections.singletonList(path));
        return cis;
    }
}

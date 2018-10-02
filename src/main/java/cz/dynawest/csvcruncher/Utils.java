package cz.dynawest.csvcruncher;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Utils
{
    static File resolvePathToUserDirIfRelative(Path path)
    {
        return path.isAbsolute() ? path.toFile() : Paths.get(System.getProperty("user.dir")).resolve(path).toFile();
    }

    static String escapeSql(String str)
    {
        return str.replace("'", "''");
    }
}

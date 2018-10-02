package cz.dynawest.csvcruncher;

import java.io.File;
import java.nio.file.Paths;

public class Utils
{
    static File resolvePathToUserDirIfRelative(String path)
    {
        return Paths.get(path).isAbsolute() ? new File(path) : new File(System.getProperty("user.dir"), path);
    }

    static String escapeSql(String str)
    {
        return str.replace("'", "''");
    }
}

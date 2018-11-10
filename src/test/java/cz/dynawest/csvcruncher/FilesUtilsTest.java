package cz.dynawest.csvcruncher;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class FilesUtilsTest
{

    @Test
    public void filterFilePaths()
    {
        Options options = new Options();
        options.setIncludePathsRegex(Pattern.compile("^foo\\..*"));
        options.setExcludePathsRegex(Pattern.compile(".*\\.bar$"));
        List<Path> paths = new ArrayList<>();
        paths.add(Paths.get("foo.bar"));
        paths.add(Paths.get("foo.foo"));
        paths.add(Paths.get("bar.foo"));
        paths.add(Paths.get("bar.bar"));

        List<Path> paths1 = FilesUtils.filterPaths(options, paths);

        assertFalse(paths1.contains(Paths.get("foo.bar")));
        assertFalse(paths1.contains(Paths.get("bar.foo")));
        assertFalse(paths1.contains(Paths.get("bar.bar")));
        assertTrue(paths1.contains(Paths.get("foo.foo")));
    }


    @Test
    public void concatFiles()
    {
    }

    @Test
    public void sortInputPaths()
    {
    }

    @Test
    public void convertResultToJson()
    {
    }

    @Test
    public void combineInputFiles()
    {
    }

    @Test
    public void parseColsFromFirstCsvLine()
    {
    }
}

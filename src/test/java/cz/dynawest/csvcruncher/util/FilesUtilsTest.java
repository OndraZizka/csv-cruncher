package cz.dynawest.csvcruncher.util;

import cz.dynawest.csvcruncher.CsvCruncherTestUtils;
import cz.dynawest.csvcruncher.Options;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import static org.junit.Assert.*;
import org.junit.Test;

public class FilesUtilsTest
{
    static Path testDataDir = CsvCruncherTestUtils.getTestDataDir();
    static Path testOutputDir = CsvCruncherTestUtils.getTestOutputDir();

    @Test
    public void filterFilePaths()
    {
        List<Path> paths = new ArrayList<>();
        paths.add(Paths.get("foo.bar"));
        paths.add(Paths.get("foo.foo"));
        paths.add(Paths.get("bar.foo"));
        paths.add(Paths.get("bar.bar"));

        Options options = new Options();
        options.setIncludePathsRegex(Pattern.compile("^foo\\..*"));
        options.setExcludePathsRegex(Pattern.compile(".*\\.bar$"));

        List<Path> paths1 = FilesUtils.filterPaths(options, paths);
        assertFalse(paths1.contains(Paths.get("foo.bar")));
        assertFalse(paths1.contains(Paths.get("bar.foo")));
        assertFalse(paths1.contains(Paths.get("bar.bar")));
        assertTrue(paths1.contains(Paths.get("foo.foo")));

        // Nulls
        options.setIncludePathsRegex(null);
        options.setExcludePathsRegex(null);

        paths1 = FilesUtils.filterPaths(options, paths);
        assertTrue(paths1.contains(Paths.get("foo.bar")));
        assertTrue(paths1.contains(Paths.get("bar.foo")));
        assertTrue(paths1.contains(Paths.get("bar.bar")));
        assertTrue(paths1.contains(Paths.get("foo.foo")));

        options.setIncludePathsRegex(null);
        options.setExcludePathsRegex(Pattern.compile(".*\\.bar$"));

        paths1 = FilesUtils.filterPaths(options, paths);
        assertFalse(paths1.contains(Paths.get("foo.bar")));
        assertTrue(paths1.contains(Paths.get("bar.foo")));
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
    public void combineInputFiles_changedSchema() throws IOException
    {
        Options options = new Options();
        options.setInputPaths(Arrays.asList(testDataDir.resolve("sample-changedSchema").toString()));
        options.setCombineDirs(Options.CombineDirectories.COMBINE_ALL_FILES);
        options.setCombineInputFiles(Options.CombineInputFiles.CONCAT);
        options.setOutputPathCsv(testOutputDir.resolve("combineInputFilesTest.csv").toString());
        options.setOverwrite(true);
        options.setInitialRowNumber(1L);
        options.setSql("SELECT * FROM concat");

        List<Path> inputPaths = Collections.singletonList(testDataDir.resolve("sample-changedSchema"));

        Map<Path, List<Path>> inputFileGroups = FilesUtils.expandFilterSortInputFilesGroups(inputPaths, options);
        Map<Path, List<Path>> fileGroups = FilesUtils.combineInputFiles(inputFileGroups, options);

        assertNotNull(fileGroups);
        assertEquals(2, fileGroups.size());

        fileGroups.forEach((concatenatedFile, sourceFiles) -> {
            assertTrue(concatenatedFile.toFile().isFile());
            assertTrue(concatenatedFile.toFile().length() > 0);
            sourceFiles.forEach(sourceFile -> sourceFile.toFile().isFile());
        });
    }

    @Test
    public void expandDirectories() {
        Options options = new Options();
        List<Path> inputPaths = Arrays.asList(testDataDir.resolve("sample-changedSchema"));
        options.setInputPaths(Collections.singletonList(inputPaths.get(0).toString()));
        options.setCombineDirs(Options.CombineDirectories.COMBINE_ALL_FILES);

        Map<Path, List<Path>> fileGroupsToConcat = FilesUtils.expandDirectories(inputPaths, options);

        assertEquals("Just one catchall group expected", 1, fileGroupsToConcat.size());
        assertNotNull("Just one catchall group expected", fileGroupsToConcat.get(null));
        assertEquals("4 files found", 4, fileGroupsToConcat.get(null).size());
    }


    @Test
    public void parseColsFromFirstCsvLine() throws IOException
    {
        Path csvFileWithHeader = testDataDir.resolve("sample-collab/session_telephony_pins/20180918-132721852.csv");
        List<String> colNames = FilesUtils.parseColsFromFirstCsvLine(csvFileWithHeader.toFile());

        // Op,id,uuid,session_id,pin,pin_type,pin_access_type,enrollment_id,created_time,modified_time
        assertEquals("Proper column count", 10, colNames.size());
        assertEquals("Col name Op", "Op", colNames.get(0));
        assertEquals("Col name id", "id", colNames.get(1));
        assertEquals("Col name uuid", "uuid", colNames.get(2));
        assertEquals("Col name modified_time", "modified_time", colNames.get(9));
    }

}

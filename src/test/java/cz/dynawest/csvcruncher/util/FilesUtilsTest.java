package cz.dynawest.csvcruncher.util;

import cz.dynawest.csvcruncher.CruncherInputSubpart;
import cz.dynawest.csvcruncher.CsvCruncherTestUtils;
import cz.dynawest.csvcruncher.Options;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
    public void deriveNameForCombinedFile()
    {
        HashMap<Path, List<Path>> fileGroup = new HashMap<>();
        fileGroup.put(Paths.get("foo"), Collections.emptyList());

        HashSet<Path> usedConcatFilePaths = new HashSet<>();
        usedConcatFilePaths.add(Paths.get("foo.csv"));

        String derivedName = FilesUtils.deriveNameForCombinedFile(fileGroup.entrySet().iterator().next(), usedConcatFilePaths);

        assertEquals("foo_1.csv", derivedName);
    }

    @Test
    public void deriveNameForCombinedFile_dir()
    {
        HashMap<Path, List<Path>> fileGroup = new HashMap<>();
        fileGroup.put(Paths.get("foo/bar.csv"), Collections.emptyList());

        HashSet<Path> usedConcatFilePaths = new HashSet<>();
        usedConcatFilePaths.add(Paths.get("foo/bar.csv"));

        String derivedName = FilesUtils.deriveNameForCombinedFile(fileGroup.entrySet().iterator().next(), usedConcatFilePaths);

        assertEquals("bar_1.csv", derivedName);
    }

    @Test
    public void getNonUsedName()
    {
        Path nonUsed;
        {
            Path path = Paths.get("some/path.csv");
            Path path_1 = Paths.get("some/path_1.csv");
            Path path_2 = Paths.get("some/path_2.csv");
            Path path2 = Paths.get("some/path2.csv");
            Path path3 = Paths.get("some/path3.csv");

            nonUsed = FilesUtils.getNonUsedName(path, Collections.emptySet());
            assertEquals(path, nonUsed);

            nonUsed = FilesUtils.getNonUsedName(path, new HashSet<>(Collections.singleton(clonePath(path))));
            assertEquals(path_1, nonUsed);

            nonUsed = FilesUtils.getNonUsedName(path, new HashSet<>(Arrays.asList(path, path_1)));
            assertEquals(path_2, nonUsed);
        }

        {
            Path path = Paths.get("some/path");
            Path path_1 = Paths.get("some/path_1");
            Path path_2 = Paths.get("some/path_2");
            Path path2 = Paths.get("some/path2");
            Path path3 = Paths.get("some/path3");

            nonUsed = FilesUtils.getNonUsedName(path, Collections.emptySet());
            assertEquals(path, nonUsed);

            nonUsed = FilesUtils.getNonUsedName(path, new HashSet<>(Collections.singleton(clonePath(path))));
            assertEquals(path_1, nonUsed);

            nonUsed = FilesUtils.getNonUsedName(path, new HashSet<>(Arrays.asList(clonePath(path), path_1)));
            assertEquals(path_2, nonUsed);
        }
    }

    private Path clonePath(Path path)
    {
        return path.resolve("./").normalize();
    }

    @Test
    public void combineInputFiles_changedSchema() throws IOException
    {
        Options options = new Options();
        options.setInputPaths(Arrays.asList(testDataDir.resolve("sample-changedSchema").toString()));
        options.setExcludePathsRegex(Pattern.compile(".*/LOAD.*\\.csv"));
        options.setCombineDirs(Options.CombineDirectories.COMBINE_ALL_FILES);
        options.setCombineInputFiles(Options.CombineInputFiles.CONCAT);
        options.setOutputPathCsv(testOutputDir.resolve("combineInputFilesTest.csv").toString());
        options.setOverwrite(true);
        options.setInitialRowNumber(1L);
        options.setSql("SELECT * FROM concat");

        List<Path> inputPaths = Collections.singletonList(testDataDir.resolve("sample-changedSchema"));

        Map<Path, List<Path>> inputFileGroups = FilesUtils.expandFilterSortInputFilesGroups(inputPaths, options);
        List<CruncherInputSubpart> inputSubparts = FilesUtils.combineInputFiles(inputFileGroups, options);

        assertNotNull(inputSubparts);
        assertEquals(2, inputSubparts.size());

        inputSubparts.forEach((inputSubpart) -> {
            assertTrue(inputSubpart.getCombinedFile().toFile().isFile());
            assertTrue(inputSubpart.getCombinedFile().toFile().length() > 0);
            inputSubpart.getCombinedFromFiles().forEach(sourceFile -> sourceFile.toFile().isFile());
        });
    }

    @Test
    public void expandDirectories() {
        Options options = new Options();
        List<Path> inputPaths = Arrays.asList(testDataDir.resolve("sample-changedSchema"));
        options.setInputPaths(Collections.singletonList(inputPaths.get(0).toString()));
        options.setIncludePathsRegex(Pattern.compile(".*\\.csv"));
        options.setExcludePathsRegex(Pattern.compile(".*/LOAD.*\\.csv"));
        options.setCombineDirs(Options.CombineDirectories.COMBINE_ALL_FILES);

        Map<Path, List<Path>> fileGroupsToConcat = FilesUtils.expandDirectories(inputPaths, options);

        assertEquals("Just one catchall group expected", 1, fileGroupsToConcat.size());
        assertNotNull("Just one catchall group expected", fileGroupsToConcat.get(null));
        assertEquals("5 files found", 5, fileGroupsToConcat.get(null).size());

        fileGroupsToConcat = FilesUtils.filterFileGroups(fileGroupsToConcat, options);

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

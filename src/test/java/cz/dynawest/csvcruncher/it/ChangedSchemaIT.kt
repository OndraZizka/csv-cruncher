package cz.dynawest.csvcruncher.it;

import cz.dynawest.csvcruncher.Cruncher;
import cz.dynawest.csvcruncher.CsvCruncherTestUtils;
import cz.dynawest.csvcruncher.Options;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;

public class ChangedSchemaIT
{
    @Before
    public void dropDbSchema() {

    }

    @Test
    public void changedSchema() throws Exception
    {
        Path testDataDir = CsvCruncherTestUtils.getTestDataDir().resolve("sample-changedSchema");
        Path testOutputDir = CsvCruncherTestUtils.getTestOutputDir().resolve("sample-changedSchema");

        Options options = new Options();
        options.setInputPaths(Arrays.asList(testDataDir.toString()));
        options.setCombineDirs(Options.CombineDirectories.COMBINE_ALL_FILES);
        options.setCombineInputFiles(Options.CombineInputFiles.CONCAT);
        options.setOutputPathCsv(testOutputDir.resolve("testResult.csv").toString());
        options.setOverwrite(true);
        options.setQueryPerInputSubpart(true); // This is key.
        options.setInitialRowNumber(1L);
        options.setSql("SELECT "+Cruncher.SQL_TABLE_PLACEHOLDER+".* FROM " + Cruncher.SQL_TABLE_PLACEHOLDER);

        // TODO: This test fails, because the tables created are NULL_1, NULL_2
        //      which is OK per se, but I need to implement dumping without -sql
        //      and dump all automaticaly.

        // TODO: Also let's switch to LogBack - JUL sucks.

        new Cruncher(options).crunch();
    }
}

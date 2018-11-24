package cz.dynawest.csvcruncher.it;

import cz.dynawest.csvcruncher.CsvCruncherTestUtils;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Test;

/**
 * The testdata contain a change in columns structure, so CSV Cruncher needs to be run with --queryPerInputSubpart.
 */
public class QueryPerInputSubpartIT
{
    Path inPath = Paths.get("src/test/data/sample-queryPerInputSubpart/oauth_consumer");


    @Test
    public void queryPerInputSubpart() throws Exception
    {
        Path outputDir = Paths.get("target/testResults/queryPerInputSubpart.csv");

        String command = "--json | --combineInputs | --queryPerInputSubpart | --rowNumbers" +
                " | -in  | " + inPath +
                // " | --exclude=.*/LOAD.*\\.csv" +
                " | -out | " + outputDir +
                " | -sql | SELECT $table.* FROM $table";

        CsvCruncherTestUtils.runCruncherWithArguments(command);
        checkOutputFiles(outputDir);
        // TODO: Add more verifications.
    }

    @Test
    public void queryPerInputSubpart_defaultSQL() throws Exception
    {
        Path outputDir = Paths.get("target/testResults/queryPerInputSubpart_defaultSQL.csv");

        String command = "--json | --combineInputs | --queryPerInputSubpart | --rowNumbers" +
                " | -in  | " + inPath +
                // " | --exclude=.*/LOAD.*\\.csv" +
                " | -out | " + outputDir;

        CsvCruncherTestUtils.runCruncherWithArguments(command);
        checkOutputFiles(outputDir);

        // TODO: Add the verifications.
    }

    @Test
    public void queryPerInputSubpart_negative() throws Exception
    {
        String command = "--json | --combineInputs | --queryPerInputSubpart | --rowNumbers" +
                " | -in  | " + inPath +
                // " | --exclude=.*/LOAD.*\\.csv" +
                " | -out | target/testResults/queryPerInputSubpart_negative.csv" +
                " | -sql | SELECT oauth_consumer.* FROM oauth_consumer";

        try {
            CsvCruncherTestUtils.runCruncherWithArguments(command);
            Assert.fail("Should have thrown IllegalArgumentException, --queryPerInputSubpart needs generic SQL.");
        }
        catch (IllegalArgumentException ex) {
            Assert.assertTrue(ex.getMessage().contains("queryPerInputSubpart"));
            Assert.assertTrue(ex.getMessage().contains("$table"));
        }
    }

    private void checkOutputFiles(Path outputDir)
    {
        Assert.assertTrue(outputDir.toFile().exists());
        Assert.assertTrue(outputDir.resolve("oauth_consumer_1.json").toFile().exists());
        Assert.assertTrue(outputDir.resolve("oauth_consumer_2.json").toFile().exists());
    }
}

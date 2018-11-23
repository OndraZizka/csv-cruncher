package cz.dynawest.csvcruncher.it;

import cz.dynawest.csvcruncher.CsvCruncherTestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * This is meant to reproduce a bug on existing environment, not to be run automatically.
 */
public class QueryPerInputSubpartIT
{
    String inPath = "src/test/data/sample-queryPerInputSubpart/oauth_consumer";


    @Test
    public void queryPerInputSubpart() throws Exception
    {
        String command = "--json | --combineInputs | --queryPerInputSubpart | --rowNumbers" +
                " | -in  | " + inPath +
                // " | --exclude=.*/LOAD.*\\.csv" +
                " | -out | target/testResults/queryPerInputSubpart.csv" +
                " | -sql | SELECT $table.* FROM $table";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        // TODO: Add the verifications.
    }

    @Test
    public void queryPerInputSubpart_defaultSQL() throws Exception
    {
        String command = "--json | --combineInputs | --queryPerInputSubpart | --rowNumbers" +
                " | -in  | " + inPath +
                // " | --exclude=.*/LOAD.*\\.csv" +
                " | -out | target/testResults/queryPerInputSubpart_defaultSQL.csv";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

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
        }
        catch (IllegalArgumentException ex) {
            Assert.assertTrue(ex.getMessage().contains("queryPerInputSubpart"));
            Assert.assertTrue(ex.getMessage().contains("$table"));
        }
    }

}

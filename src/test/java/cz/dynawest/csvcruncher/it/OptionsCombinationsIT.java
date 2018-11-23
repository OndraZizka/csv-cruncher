package cz.dynawest.csvcruncher.it;

import cz.dynawest.csvcruncher.CsvCruncherTestUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Paths;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TODO: Add the verifications.
 */
public class OptionsCombinationsIT
{
    @BeforeClass
    public static void deletePreviousResults(){
        CsvCruncherTestUtils.getTestOutputDir().toFile().delete();
    }

    /**
     *  ${testRunCmd}
     *  --json=entries
     *  --rowNumbers
     *  -in | src/test/data/eapBuilds.csv
     *  -out | target/results/result.csv
     *  -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale,
     *  CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower
     *  FROM eapBuilds ORDER BY deployDur'
     */
    @Test
    public void testSimple() throws Exception
    {
        //String cmdBase = System.getProperty("test.cruncher.runCmd");
        //String cmdSplit = Arrays.stream(cmdBase.split(" ")).collect(Collectors.joining("|"));

        String command = //cmdSplit +
                " | --json=entries" +
                " | --rowNumbers" +
                " | -in |  | src/test/data/eapBuilds.csv" +
                " | -out | target/results/result.csv" +
                " | -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale," +
                "  CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower" +
                "  FROM eapBuilds ORDER BY deployDur";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        // TODO: Add the verifications.
    }

    @Test
    public void combineInputFile() throws Exception
    {
        String command = "--json=entries" +
                " |  --rowNumbers" +
                " |  --combineInputs" +
                " |  -in | src/test/data/eapBuilds.csv" +
                " |  -out | target/results/result.csv" +
                " |  -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale," +
                "   CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower" +
                "   FROM eapBuilds ORDER BY deployDur";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        // TODO: Add the verifications.
    }

    @Test
    public void combineInputFiles_sort() throws Exception
    {
        String command = "--json=entries" +
                " |  --rowNumbers" +
                " |  --combineInputs=concat" +
                " |  --combineDirs=all" +
                " |  --sortInputFileGroups" +
                " |  -in | src/test/data/sample-multiFile-all" +
                " |  -out | target/testResults-sort/result.csv | --overwrite" +
                " |  -sql | SELECT sample_multifile_all.* FROM sample_multifile_all";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        File csvFile = Paths.get("target/testResults-sort/result.csv").toFile();
        checkThatIdsAreIncrementing(csvFile, 3);
    }

    /**
     * Reads the given CSV file, skips the first line, and then checks if the 2nd column is an incrementing number
     * (just like in the input files).
     */
    private void checkThatIdsAreIncrementing(File csvFile, int columnOffset1Based)
    {
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile));)
        {
            reader.readLine();

            Integer previousId = null;
            String line;
            while (null != (line = reader.readLine())) {
                //System.out.println(":: " + line);
                String[] values = StringUtils.splitPreserveAllTokens(line, ",");
                String idStr = values[columnOffset1Based - 1];
                int id = Integer.parseInt(idStr);
                if (previousId != null)
                    Assert.assertEquals(previousId + 1, id);
                previousId = id;
            }
        }
        catch (Exception ex) {
            throw new RuntimeException("Unexpected error parsing the CSV: " + ex.getMessage(), ex);
        }
    }

    @Test
    public void combine_perRootSubDir() throws Exception
    {
        String command = "--json=entries" +
                " |  --rowNumbers" +
                " |  --combineInputs=concat" +
                " |  --combineDirs=all" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -in | src/test/data/sampleMultiFilesPerDir/apollo_session_enrollment/" +
                " |  -out | target/results/result.csv" +
                " |  -sql | SELECT session_uid, name, session_type, created_time, modified_date" +
                " |  FROM concat ORDER BY session_type, created_time DESC";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        // TODO: Add the verifications.
    }

    @Test
    public void combine_selectStar_negative() throws Exception
    {
        String command = "--json | --combineInputs | --rowNumbers" +
                " |  -sql | SELECT * FROM session_telephony_pins" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -in | src/test/data/sampleMultiFilesPerDir/session_telephony_pins/" +
                " |  -out | target/results/session_telephony_pins.csv";

        try {
            CsvCruncherTestUtils.runCruncherWithArguments(command);
        }
        catch(Exception ex) { /**/ }
    }

    @Test
    public void combine_selectStar_qualified() throws Exception
    {
        String command = "--json | --combineInputs | --rowNumbers" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -sql | SELECT session_telephony_pins.* FROM session_telephony_pins" +
                " |  -in  | src/test/data/sampleMultiFilesPerDir/session_telephony_pins/" +
                " |  -out | target/results/session_telephony_pins.csv";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        // TODO: Add the verifications.
    }

    @Test
    public void collab_ApolloRecGroup() throws Exception
    {
        String command = "--json | --combineInputs" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -sql | SELECT * FROM apollo_recording_group" +
                " |  -in  | src/test/data/sample-collab/apollo_recording_group/" +
                " |  -out | target/results/apollo_recording_group.csv";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        // TODO: Add the verifications.
    }

    @Test
    public void collab_SessTelPins() throws Exception
    {
        String command = "--json | --combineInputs" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -in  | src/test/data/sample-collab/session_telephony_pins/" +
                " |  -out | target/results/session_telephony_pins.csv" +
                " |  -sql | SELECT * FROM session_telephony_pins";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        // TODO: Add the verifications.
    }

    @Test
    public void testVersion() throws Exception
    {
        String command = "-v";

        CsvCruncherTestUtils.runCruncherWithArguments(command);
    }

    @Test
    public void testHelp() throws Exception
    {
        String command = "-h";

        CsvCruncherTestUtils.runCruncherWithArguments(command);
    }

}

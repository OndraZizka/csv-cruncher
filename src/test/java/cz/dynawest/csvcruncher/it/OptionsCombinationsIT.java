package cz.dynawest.csvcruncher.it;

import cz.dynawest.csvcruncher.CsvCruncherTestUtils;
import cz.dynawest.csvcruncher.util.FilesUtils;
import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
        String command =
                " | --rowNumbers" +
                " | -in |  | src/test/data/eapBuilds.csv" +
                " | -out | target/testResults/testSimple.csv" +
                " | -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale," +
                "  CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower" +
                "  FROM eapBuilds ORDER BY deployDur";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        File resultCsv = Paths.get("target/testResults/testSimple.csv").toFile();
        Assert.assertTrue(resultCsv.exists());

        List<String> columnNames = FilesUtils.parseColumnsFromFirstCsvLine(resultCsv);
        Assert.assertEquals("Column names fit", columnNames.size(), 9);
        Assert.assertEquals(columnNames.get(8).toLowerCase(), "warmupslower");

        // TODO: Add content verifications.
    }

    @Test
    public void testSimpleJson() throws Exception
    {
        String command =
                " | --json=entries" +
                " | --rowNumbers" +
                " | -in |  | src/test/data/eapBuilds.csv" +
                " | -out | target/testResults/testSimple.csv" +
                " | -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale," +
                "  CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower" +
                "  FROM eapBuilds ORDER BY deployDur";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        File resultCsv = Paths.get("target/testResults/testSimple.json").toFile();
        Assert.assertTrue(resultCsv.exists());

        // TODO: Add JSON verifications.
    }

    @Test
    public void combineInputFile() throws Exception
    {
        String command = //"--json=entries" +
                " |  --rowNumbers" +
                " |  --combineInputs" +
                " |  -in | src/test/data/eapBuilds.csv" +
                " |  -out | target/testResults/combineInputFile.csv" +
                " |  -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale," +
                "   CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower" +
                "   FROM eapBuilds ORDER BY deployDur";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        File resultCsv = Paths.get("target/testResults/combineInputFile.csv").toFile();
        Assert.assertTrue(resultCsv.exists());

        List<String> columnNames = FilesUtils.parseColumnsFromFirstCsvLine(resultCsv);
        Assert.assertEquals("Column names fit", columnNames.size(), 9);
        Assert.assertEquals(columnNames.get(8).toLowerCase(), "warmupslower");
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
                " |  -out | target/testResults/combineInputFiles_sort.csv | --overwrite" +
                " |  -sql | SELECT sample_multifile_all.* FROM sample_multifile_all";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        File csvFile = Paths.get("target/testResults/combineInputFiles_sort.csv").toFile();
        CsvCruncherTestUtils.checkThatIdsAreIncrementing(Collections.singletonList(csvFile), 3, true);
    }

    @Test
    @Ignore("Not yet implemented")
    public void combine_perRootSubDir() throws Exception
    {
        String command = "--json=entries" +
                " |  --rowNumbers" +
                " |  --combineInputs=concat" +
                " |  --combineDirs=perInputSubdir" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -in | src/test/data/sample-collab/" +
                " |  -out | target/testResults/combine_perRootSubDir.csv" +
                " |  -sql | SELECT session_uid, name, session_type, created_time, modified_date" +
                "    FROM concat ORDER BY session_type, created_time DESC";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        File resultCsv = Paths.get("target/testResults/combine_perRootSubDir.csv").toFile();
        Assert.assertTrue(resultCsv.exists());

        List<String> columnNames = FilesUtils.parseColumnsFromFirstCsvLine(resultCsv);
        Assert.assertEquals("Column names fit", columnNames.size(), 5);
        Assert.assertEquals(columnNames.get(4).toLowerCase(), "modified_date");

        // TODO: Add content verifications.
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
        catch(IllegalArgumentException ex) { /**/ }
    }

    @Test
    public void combine_selectStar_qualified() throws Exception
    {
        // cruncherCounter, Op,id,uuid,session_id,pin,pin_type,pin_access_type,enrollment_id,created_time,modified_time
        // 123456..., I,9999,950c2668-794b-4cf9-894a-af6aea5bf5d5,1000,1234567891,0,0,,2018-08-02 07:34:55.303000,2018-08-02 07:34:55.303000

        String inputCsv = "target/testResults/combine_selectStar_qualified.csv";

        String command = "--json | --combineInputs | --rowNumbers" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -sql | SELECT session_telephony_pins.* FROM session_telephony_pins" +
                " |  -in  | src/test/data/sampleMultiFilesPerDir/session_telephony_pins/" +
                " |  -out | " + inputCsv;

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        File resultCsv = Paths.get(inputCsv).toFile();
        Assert.assertTrue(resultCsv.exists());

        List<String> columnNames = FilesUtils.parseColumnsFromFirstCsvLine(resultCsv);
        Assert.assertEquals("Column names fit", columnNames.size(), 10);
        Assert.assertEquals(columnNames.get(9).toLowerCase(), "modified_time");

        String shouldBeNull = CsvCruncherTestUtils.getCsvCellValue(resultCsv, 1, 8);
        Assert.assertEquals("row 1, col 9 should be null", null, shouldBeNull);
    }


    @Test
    public void collab_ApolloRecGroup() throws Exception
    {
        String command = "--json | --combineInputs" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -sql | SELECT * FROM apollo_recording_group" +
                " |  -in  | src/test/data/sample-collab/apollo_recording_group/" +
                " |  -out | target/testResults/apollo_recording_group.csv";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        // TODO: Add the verifications.
    }

    @Test
    public void collab_SessTelPins() throws Exception
    {
        // Op,id,uuid,session_id,pin,pin_type,pin_access_type,enrollment_id,created_time,modified_time
        // I,9999,950c2668-794b-4cf9-894a-af6aea5bf5d5,1000,1234567891,0,0,,2018-08-02 07:34:55.303000,2018-08-02 07:34:55.303000

        String command = "--json | --combineInputs" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -in  | src/test/data/sample-collab/session_telephony_pins/" +
                " |  -out | target/testResults/session_telephony_pins.csv" +
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

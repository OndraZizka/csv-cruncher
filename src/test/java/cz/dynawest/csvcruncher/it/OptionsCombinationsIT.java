package cz.dynawest.csvcruncher.it;

import cz.dynawest.csvcruncher.App;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class OptionsCombinationsIT extends CsvCruncherITBase
{
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

        runCruncherWithArguments(command);
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

        runCruncherWithArguments(command);
    }

    @Test
    public void combine_perRootSubDir() throws Exception
    {
        String command = "--json=entries" +
                " |  --rowNumbers" +
                " |  --combineInputs=concat" +
                " |  --combineDirs=all" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -in | src/test/data/sampleMultiFilesPerDir/apollo_session/" +
                " |  -out | target/results/result.csv" +
                " |  -sql | SELECT session_uid, name, session_type, created_time, modified_date" +
                " |  FROM concat ORDER BY session_type, created_time DESC";

        runCruncherWithArguments(command);
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
            runCruncherWithArguments(command);
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

        runCruncherWithArguments(command);
    }

    @Test
    public void collab_ApolloRecGroup() throws Exception
    {
        String command = "--json | --combineInputs" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -sql | SELECT * FROM apollo_recording_group" +
                " |  -in  | src/test/data/sample-collab/apollo_recording_group/" +
                " |  -out | target/results/apollo_recording_group.csv";

        runCruncherWithArguments(command);
    }

    @Test
    public void collab_SessTelPins() throws Exception
    {
        String command = "--json | --combineInputs" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -in  | src/test/data/sample-collab/session_telephony_pins/" +
                " |  -out | target/results/session_telephony_pins.csv" +
                " |  -sql | SELECT * FROM session_telephony_pins";

        runCruncherWithArguments(command);
    }

    @Test
    public void testVersion() throws Exception
    {
        String command = "-v";

        runCruncherWithArguments(command);
    }

    @Test
    public void testHelp() throws Exception
    {
        String command = "-h";

        runCruncherWithArguments(command);
    }


    /**
     * Runs CSV Cruncher with the guven command, which is | separated arguments.
     */
    private void runCruncherWithArguments(String command) throws Exception
    {
        List<String> collect = Arrays.stream(command.split("\\|")).map(String::trim).filter(x -> !x.isEmpty()).collect(Collectors.toList());
        String[] args = collect.toArray(new String[0]);

        App.mainNoExit(args);
    }
}

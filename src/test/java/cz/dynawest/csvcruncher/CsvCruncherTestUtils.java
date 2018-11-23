package cz.dynawest.csvcruncher;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CsvCruncherTestUtils
{
    /**
     * @return Path to the default test data dir.
     */
    public static Path getTestDataDir() {
        return Paths.get(System.getProperty("user.dir")).resolve("src/test/data/");
    }

    /**
     * @return Path to the default test output dir.
     */
    public static Path getTestOutputDir() {
        return Paths.get(System.getProperty("user.dir")).resolve("target/testResults/");
    }


    /**
     * Runs CSV Cruncher with the guven command, which is | separated arguments.
     */
    public static void runCruncherWithArguments(String command) throws Exception
    {
        List<String> collect = Arrays.stream(command.split("\\|")).map(String::trim).filter(x -> !x.isEmpty()).collect(Collectors.toList());
        String[] args = collect.toArray(new String[0]);

        App.mainNoExit(args);
    }
}

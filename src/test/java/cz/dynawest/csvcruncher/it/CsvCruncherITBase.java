package cz.dynawest.csvcruncher.it;

import java.nio.file.Path;
import java.nio.file.Paths;

public class CsvCruncherITBase
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
}

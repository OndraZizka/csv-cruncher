package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import org.junit.jupiter.api.Test
import java.nio.file.Paths

class BasicIT {

    @Test
    fun singleImportSingleExport() {
        var inPath = Paths.get("src/test/data/eapBuilds.csv")
        val outputDir = Paths.get("target/results/queryPerInputSubpart.csv")
        val command =
            """--json=entries | --rowNumbers | -in  | $inPath | -out | $outputDir | -sql |
                 SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale,
                 CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower
                 FROM eapBuilds ORDER BY deployDur"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }

    @Test
    fun realData_redditAll() {
        var inPath = Paths.get("src/test/data/json/redditAll.json")
        val outputPath = Paths.get("target/results/redditAll.csv")
        val command =
            """-in | $inPath | -itemsAt | /data/children | -out | $outputPath | -sql |
                 SELECT * FROM redditAll """
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }
}
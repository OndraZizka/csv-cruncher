package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

class BasicIT {

    @Test
    fun queryPerInputSubpart() {
        var inPath = Paths.get("src/test/data/eapBuilds.csv")
        val outputDir = Paths.get("target/results/queryPerInputSubpart.csv")
        val command =
            """--json=entries | --rowNumbers | -in  | $inPath | -out | $outputDir | -sql |
                 SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale,
                 CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower
                 FROM eapBuilds ORDER BY deployDur"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }
}
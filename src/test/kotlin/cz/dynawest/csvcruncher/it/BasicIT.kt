package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

class BasicIT {

    /*
    * T--json=entries
03:38:47.383 [main] DEBUG cz.dynawest.csvcruncher.app.OptionsParser -  * --rowNumbers
03:38:47.383 [main] DEBUG cz.dynawest.csvcruncher.app.OptionsParser -  * -in
03:38:47.384 [main] DEBUG cz.dynawest.csvcruncher.app.OptionsParser -  * src/test/data/eapBuilds.csv
03:38:47.384 [main] DEBUG cz.dynawest.csvcruncher.app.OptionsParser -  * -out
03:38:47.384 [main] DEBUG cz.dynawest.csvcruncher.app.OptionsParser -  * target/results/result.csv
03:38:47.384 [main] DEBUG cz.dynawest.csvcruncher.app.OptionsParser -  * -sql
03:38:47.384 [main] DEBUG cz.dynawest.csvcruncher.app.OptionsParser -  * SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale,
                                CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower
                                FROM eapBuilds ORDER BY deployDur
     */

    var inPath = Paths.get("src/test/data/eapBuilds.csv")

    @Test
    fun queryPerInputSubpart() {
        val outputDir = Paths.get("target/results/queryPerInputSubpart.csv")
        val command =
            """--json=entries | --rowNumbers | -in  | $inPath | -out | $outputDir | -sql |
                 SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale,
                 CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower
                 FROM eapBuilds ORDER BY deployDur"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }
}
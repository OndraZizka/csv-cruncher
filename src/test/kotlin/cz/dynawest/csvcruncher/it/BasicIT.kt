package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.nio.file.Paths
import kotlin.io.path.deleteIfExists

class BasicIT {

    @Test
    fun singleImportSingleExportWithJson() {
        var inPath = Paths.get("src/test/data/eapBuilds.csv")
        val outputPath = Paths.get("target/results/queryPerInputSubpart.csv")
        outputPath.deleteIfExists()

        val command =
            """--json=entries | --rowNumbers | -in  | $inPath | -out | $outputPath | -sql |
                 SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale,
                 CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower
                 FROM eapBuilds ORDER BY deployDur"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(outputPath).exists().isNotEmptyFile()
    }

    @Test
    fun singleImportSingleExport_defaultSql() {
        var inPath = Paths.get("src/test/data/eapBuilds.csv")
        val outputPath = Paths.get("target/results/defaultSql.csv")
        outputPath.deleteIfExists()

        val command = """-in  | $inPath | -out | $outputPath"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(outputPath).exists().isNotEmptyFile()
    }

    @Test
    fun singleImportSingleExport_jsonFileExtension() {
        var inPath = Paths.get("src/test/data/eapBuilds.csv")
        val outputPath = Paths.get("target/results/singleImportSingleExport_jsonFileExtension.json")
        outputPath.deleteIfExists()

        val command = """-in  | $inPath | -out | $outputPath"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(outputPath).exists().isNotEmptyFile()
    }

    @Test
    fun test_initSqlScript() {
        var inPath = Paths.get("src/test/data/eapBuilds.csv")
        val outputPath = Paths.get("target/results/test_initSqlScript.csv")
        outputPath.deleteIfExists()

        val initSqlFile = Paths.get("src/test/data/init.sql")
        val command =
            """-initSql | $initSqlFile | -in | $inPath | -out | $outputPath | -sql | SELECT jobName FROM eapBuilds LIMIT 1"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(outputPath).exists().isNotEmptyFile()
    }

    @Test
    fun realData_json_redditAll() {
        var inPath = Paths.get("src/test/data/json/redditAll.json")
        val outputPath = Paths.get("target/results/redditAll.csv")
        outputPath.deleteIfExists()

        val command = """-in | $inPath | -itemsAt | /data/children | -out | $outputPath | -sql | SELECT * FROM REDDITALL_JSON"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(outputPath).exists().isNotEmptyFile()
    }

    @Test
    fun realData_json_noParentDirOfOutput() {
        var inPath = Paths.get("src/test/data/json/redditAll.json")
        val outputPath = Paths.get("test-DELETE.csv")
        outputPath.deleteIfExists()

        val command = """-in | $inPath | -itemsAt | /data/children | -out | $outputPath | -sql | SELECT * FROM REDDITALL_JSON"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }

    @Test @Disabled("Currently we just overwrite to simplify experimenting. Maybe let's have --noOverwrite?")
    fun realData_json_overwrite() {
        var inPath = Paths.get("src/test/data/json/redditAll.json")
        val outputPath = Paths.get("target/results/overwriteTestFile.csv")
        outputPath.deleteIfExists()

        val command = """-in | $inPath | -itemsAt | /data/children | -out | $outputPath | -sql | SELECT * FROM REDDITALL_JSON"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        try {
            CsvCruncherTestUtils.runCruncherWithArguments(command)
            fail("Should have failed.")
        }
        catch (ex: Exception) {}
    }
}
package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import cz.dynawest.csvcruncher.CsvCruncherTestUtils.testDataDir
import cz.dynawest.csvcruncher.defaultCsvOutputPath
import cz.dynawest.csvcruncher.defaultJsonOutputPath
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.api.assertThrows
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.deleteIfExists
import kotlin.io.path.pathString

class BasicIntegTest {

    @BeforeEach
    @AfterEach
    fun cleanup(testInfo: TestInfo) {
        testInfo.defaultCsvOutputPath().deleteIfExists()
    }


    @Test
    fun singleImportSingleExportWithJson(testInfo: TestInfo) {
        val inPath = Paths.get("$testDataDir/eapBuilds.csv")
        val outputPath = testInfo.defaultCsvOutputPath()

        val command =
            """--json=entries | --rowNumbers | -in  | $inPath | -out | $outputPath | -sql |
                 SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale,
                 CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower
                 FROM eapBuilds ORDER BY deployDur"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(outputPath).exists().isNotEmptyFile()
    }

    @Test
    fun singleImportSingleExport_defaultSql(testInfo: TestInfo) {
        val inPath = Paths.get("$testDataDir/eapBuilds.csv")
        val outputPath = testInfo.defaultCsvOutputPath()

        val command = """-in  | $inPath | -out | $outputPath"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(outputPath).exists().isNotEmptyFile()
    }

    @Test
    fun singleImportSingleExport_jsonFileExtension(testInfo: TestInfo) {
        val inPath = Paths.get("$testDataDir/eapBuilds.csv")
        val outputPath = testInfo.defaultJsonOutputPath()

        val command = """-in  | $inPath | -out | $outputPath"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(outputPath).exists().isNotEmptyFile()
    }

    @Test
    fun test_initSqlScript(testInfo: TestInfo) {
        val inPath = Paths.get("$testDataDir/eapBuilds.csv")
        val outputPath = testInfo.defaultCsvOutputPath()

        val initSqlFile = Paths.get("$testDataDir/init.sql")
        val command =
            """-initSql | $initSqlFile | -in | $inPath | -out | $outputPath | -sql | SELECT jobName FROM eapBuilds LIMIT 1"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(outputPath).exists().isNotEmptyFile()
    }

    @Test
    fun realData_json_redditAll(testInfo: TestInfo) {
        val inPath = Paths.get("$testDataDir/json/redditAll.json")
        val outputPath = testInfo.defaultCsvOutputPath()

        val command = """-in | $inPath | -itemsAt | /data/children | -out | $outputPath | -sql | SELECT * FROM REDDITALL_JSON"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(outputPath).exists().isNotEmptyFile()
    }

    @Test
    fun test_basic_json_types(testInfo: TestInfo) {
        val inPath = Paths.get("$testDataDir/json/jsonTypes_all.json")
        val outputPath = testInfo.defaultCsvOutputPath()

        val command = """-in | $inPath | -itemsAt | / | -out | $outputPath | -sql | SELECT * FROM jsonTypes_all_json"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(outputPath).exists().isNotEmptyFile()
    }

    @Test
    fun test_basic_json_noParentDirOfOutput(testInfo: TestInfo) {
        val inPath = Paths.get("$testDataDir/json/redditAll.json")
        val outputPath = testInfo.defaultJsonOutputPath()
        Path.of(outputPath.pathString + ".csv").deleteIfExists() // Temp file can remain after debugging

        val command = """-in | $inPath | -itemsAt | /data/children | -out | $outputPath | -sql | SELECT * FROM REDDITALL_JSON"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }

    @Test @Disabled("Currently we just overwrite to simplify experimenting. Maybe let's have --noOverwrite?")
    fun testDoesNotOverwrite(testInfo: TestInfo) {
        val inPath = Paths.get("$testDataDir/json/redditAll.json")
        val outputPath = testInfo.defaultCsvOutputPath()

        val command = """-in | $inPath | -itemsAt | /data/children | -out | $outputPath | -sql | SELECT * FROM REDDITALL_JSON"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThrows<Exception> {
            CsvCruncherTestUtils.runCruncherWithArguments(command)
        }
    }
}
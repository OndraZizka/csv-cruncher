package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CrucherConfigException
import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import cz.dynawest.csvcruncher.util.FilesUtils.parseColumnsFromFirstCsvLine
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Paths

/**
 * TODO: Add the verifications.
 */
class OptionsCombinationsTest {
    /**
     * ${testRunCmd}
     * --json=entries
     * --rowNumbers
     * -in | src/test/data/eapBuilds.csv
     * -out | target/results/result.csv
     * -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale,
     * CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower
     * FROM eapBuilds ORDER BY deployDur'
     */
    @Test
    fun testSimple() {
        val command = " | --rowNumbers" +
                " | -in |  | src/test/data/eapBuilds.csv" +
                " | -out | target/testResults/testSimple.csv" +
                " | -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale," +
                "  CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower" +
                "  FROM eapBuilds ORDER BY deployDur"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        val resultCsv = Paths.get("target/testResults/testSimple.csv").toFile()
        assertTrue(resultCsv.exists())
        val columnNames = parseColumnsFromFirstCsvLine(resultCsv)
        assertEquals(columnNames.size.toLong(), 9, "Column names fit")
        assertEquals(columnNames[8].lowercase(), "warmupslower")

        // TODO: Add content verifications.
    }

    @Test
    fun testSimpleJson() {
        val command = " | --json=entries" +
                " | --rowNumbers" +
                " | -in |  | src/test/data/eapBuilds.csv" +
                " | -out | target/testResults/testSimple.csv" +
                " | -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale," +
                "  CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower" +
                "  FROM eapBuilds ORDER BY deployDur"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        val resultCsv = Paths.get("target/testResults/testSimple.json").toFile()
        assertTrue(resultCsv.exists())

        // TODO: Add JSON verifications.
    }

    @Test
    fun testOutputToStdout() {
        val command = " | --json=entries" +
                " | --rowNumbers" +
                " | -in |  | src/test/data/eapBuilds.csv" +
                " | -out | -" +
                " | -sql | SELECT CONCAT('This should appear on STDOUT. ', jobName) AS msg" +
                "  FROM eapBuilds ORDER BY deployDur LIMIT 3"
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        // If printed to stdout, the output goes to a temp file which is deleted at the end.
        val resultCsv = Paths.get("target/testResults/testOutputToStdout.json").toFile()
        assertTrue(!resultCsv.exists())
    }

    @Test @Disabled("CHARACTER bug - https://github.com/OndraZizka/csv-cruncher/issues/122")
    fun regression_i122_stringReducedToCharacter() {
        val command = " | --json=entries" +
                " | --rowNumbers" +
                " | -in |  | src/test/data/eapBuilds.csv" +
                " | -out | -" +
                " | -sql | SELECT 'This should appear on STDOUT' AS msg" +
                "  FROM eapBuilds ORDER BY deployDur LIMIT 3"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }

    @Test
    fun combineInputFile() {
        val command =
                " |  --rowNumbers" +
                " |  --combineInputs" +
                " |  -in | src/test/data/eapBuilds.csv" +
                " |  -out | target/testResults/combineInputFile.csv" +
                " |  -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale," +
                "   CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower" +
                "   FROM eapBuilds ORDER BY deployDur"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        val resultCsv = Paths.get("target/testResults/combineInputFile.csv").toFile()
        assertTrue(resultCsv.exists())
        val columnNames = parseColumnsFromFirstCsvLine(resultCsv)
        assertEquals(columnNames.size.toLong(), 9, "Column names fit")
        assertEquals(columnNames[8].lowercase(), "warmupslower")
    }

    @Test
    fun combineInputDir_defaultSql() {
        val command =
                " |  -in | src/test/data/sample-multiFile-all" +
                " |  --combineInputs" +
                " |  -out | target/testResults/combineInputDir.csv"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        val resultCsv = Paths.get("target/testResults/combineInputDir.csv").toFile()
        assertTrue(resultCsv.exists())
        val columnNames = parseColumnsFromFirstCsvLine(resultCsv)
        assertEquals(columnNames.size.toLong(), 9, "Column names fit")
        assertEquals(columnNames[8].lowercase(), "warmupslower")
    }

    @Test
    fun combineInputDir_JsonAndCsv_defaultSql_issue149() {
        val command =
                " |  -in | src/test/data/sample-multiFile-json+csv" +
                " |  --combineInputs" +
                " |  -out | target/testResults/combineInputDir_json+csv.csv"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        val resultCsv = Paths.get("target/testResults/combineInputDir_json+csv.csv").toFile()
        assertTrue(resultCsv.exists())
        val columnNames = parseColumnsFromFirstCsvLine(resultCsv)
        assertEquals(columnNames.size.toLong(), 9, "Column names fit")
        assertEquals(columnNames[8].lowercase(), "warmupslower")
    }

    @Test
    fun combineInputFiles_sort() {
        val command = "--json=entries" +
                " |  --rowNumbers" +
                " |  --combineInputs=concat" +
                " |  --combineDirs=all" +
                " |  --sortInputFileGroups" +
                " |  -in | src/test/data/sample-multiFile-all" +
                " |  -out | target/testResults/combineInputFiles_sort.csv | --overwrite" +
                " |  -sql | SELECT sample_multifile_all.* FROM sample_multifile_all"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        val csvFile = Paths.get("target/testResults/combineInputFiles_sort.csv").toFile()
        CsvCruncherTestUtils.checkThatIdsAreIncrementing(listOf(csvFile), 3, true)
    }

    @Test
    @Disabled("Not yet implemented")
    fun combine_perRootSubDir() {
        val command = "--json=entries" +
                " |  --rowNumbers" +
                " |  --combineInputs=concat" +
                " |  --combineDirs=perInputSubdir" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -in | src/test/data/sample-collab/" +
                " |  -out | target/testResults/combine_perRootSubDir.csv" +
                " |  -sql | SELECT session_uid, name, session_type, created_time, modified_date" +
                "    FROM concat ORDER BY session_type, created_time DESC"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        val resultCsv = Paths.get("target/testResults/combine_perRootSubDir.csv").toFile()
        assertTrue(resultCsv.exists())
        val columnNames = parseColumnsFromFirstCsvLine(resultCsv)
        assertEquals(columnNames.size.toLong(), 5, "Column names fit")
        assertEquals(columnNames[4].lowercase(), "modified_date")

        // TODO: Add content verifications.
    }

    @Test
    fun combine_selectStar_negative() {
        val command = "--json | --combineInputs | --rowNumbers" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -in | src/test/data/sampleMultiFilesPerDir/session_telephony_pins/" +
                " |  -out | target/results/session_telephony_pins.csv" +
                " |  -sql | SELECT * FROM session_telephony_pins"

        assertThrows<CrucherConfigException> { CsvCruncherTestUtils.runCruncherWithArguments(command) }
    }

    @Test
    fun combine_selectStar_qualified() {
        // cruncherCounter, Op,id,uuid,session_id,pin,pin_type,pin_access_type,enrollment_id,created_time,modified_time
        // 123456..., I,9999,950c2668-794b-4cf9-894a-af6aea5bf5d5,1000,1234567891,0,0,,2018-08-02 07:34:55.303000,2018-08-02 07:34:55.303000
        val inputCsv = "target/testResults/combine_selectStar_qualified.csv"
        val command = "--json | --combineInputs | --rowNumbers" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -in  | src/test/data/sampleMultiFilesPerDir/session_telephony_pins/" +
                " |  -out | $inputCsv" +
                " |  -sql | SELECT session_telephony_pins.* FROM session_telephony_pins"

        CsvCruncherTestUtils.runCruncherWithArguments(command)
        val resultCsv = Paths.get(inputCsv).toFile()
        assertTrue(resultCsv.exists())
        val columnNames = parseColumnsFromFirstCsvLine(resultCsv)
        assertEquals(columnNames.size.toLong(), 10, "Column names fit")
        assertEquals(columnNames[9].lowercase(), "modified_time")
        val shouldBeNull = CsvCruncherTestUtils.getCsvCellValue(resultCsv, 1, 8)
        assertEquals(null, shouldBeNull, "row 1, col 9 should be null")
    }

    @Test
    fun collab_ApolloRecGroup() {
        val command = "--json | --combineInputs" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -in  | src/test/data/sample-collab/apollo_recording_group/" +
                " |  -out | target/testResults/apollo_recording_group.csv" +
                " |  -sql | SELECT * FROM apollo_recording_group"

        CsvCruncherTestUtils.runCruncherWithArguments(command)

        // TODO: Add the verifications.
    }

    @Test
    fun collab_SessTelPins() {
        // Op,id,uuid,session_id,pin,pin_type,pin_access_type,enrollment_id,created_time,modified_time
        // I,9999,950c2668-794b-4cf9-894a-af6aea5bf5d5,1000,1234567891,0,0,,2018-08-02 07:34:55.303000,2018-08-02 07:34:55.303000
        val command = "--json | --combineInputs" +
                " |  --exclude=.*/LOAD.*\\.csv" +
                " |  -in  | src/test/data/sample-collab/session_telephony_pins/" +
                " |  -out | target/testResults/session_telephony_pins.csv" +
                " |  -sql | SELECT * FROM session_telephony_pins"
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        // TODO: Add the verifications.
    }

    @Test
    fun testVersion() {
        val command = "-v"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }

    @Test
    fun testHelp() {
        val command = "-h"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }

    companion object {
        @BeforeAll
        @JvmStatic
        fun deletePreviousResults() {
            CsvCruncherTestUtils.testOutputDir.toFile().delete()
        }
    }
}
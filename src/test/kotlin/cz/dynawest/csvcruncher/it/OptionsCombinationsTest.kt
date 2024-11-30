package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CrucherConfigException
import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import cz.dynawest.csvcruncher.CsvCruncherTestUtils.testDataDir
import cz.dynawest.csvcruncher.CsvCruncherTestUtils.testOutputDir
import cz.dynawest.csvcruncher.util.FilesUtils.parseColumnsFromFirstCsvLine
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertIterableEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.api.assertThrows
import java.nio.file.Paths

/**
 * TODO: Add the verifications.
 */
class OptionsCombinationsTest {

    @Test
    fun testSimple(testInfo: TestInfo) {
        val command = " | --rowNumbers" +
                " | -in  | $testDataDir/eapBuilds.csv" +
                " | -out | $testOutputDir/testSimple.csv" +
                " | -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale," +
                "  CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower" +
                "  FROM eapBuilds ORDER BY deployDur"
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        val resultCsv = Paths.get("$testOutputDir/testSimple.csv").toFile()
        assertTrue(resultCsv.exists())
        val columnNames = parseColumnsFromFirstCsvLine(resultCsv)
        assertEquals(9, columnNames.size.toLong(), "Column names fit")
        assertEquals("warmupslower", columnNames[8].lowercase())

        // TODO: Add content verifications.
    }

    @Test
    fun testSimpleJson(testInfo: TestInfo) {
        val command = " | --json=entries" +
                " | --rowNumbers" +
                " | -in  | $testDataDir/eapBuilds.csv" +
                " | -out | $testOutputDir/testSimple.csv" +
                " | -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale," +
                "  CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower" +
                "  FROM eapBuilds ORDER BY deployDur"
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        val resultCsv = Paths.get("$testOutputDir/testSimple.json").toFile()
        assertTrue(resultCsv.exists())

        // TODO: Add JSON verifications.
    }

    @Test
    fun testOutputToStdout(testInfo: TestInfo) {
        val command = " | --json=entries" +
                " | --rowNumbers" +
                " | -in  | $testDataDir/eapBuilds.csv" +
                " | -out | -" +
                " | -sql | SELECT CONCAT('This should appear on STDOUT. ', jobName) AS msg" +
                "  FROM eapBuilds ORDER BY deployDur LIMIT 3"
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        // If printed to stdout, the output goes to a temp file which is deleted at the end.
        assertTrue(!Paths.get("$testOutputDir/testOutputToStdout.json").toFile().exists())
        assertTrue(!Paths.get("$testOutputDir/-.json").toFile().exists())
        assertTrue(!Paths.get("$testOutputDir/-").toFile().exists())
        assertTrue(!Paths.get("-").toFile().exists())
    }

    @Test @Disabled("CHARACTER bug - https://github.com/OndraZizka/csv-cruncher/issues/122")
    fun regression_i122_stringReducedToCharacter(testInfo: TestInfo) {
        val command = " | --json=entries" +
                " | --rowNumbers" +
                " | -in  | $testDataDir/eapBuilds.csv" +
                " | -out | -" +
                " | -sql | SELECT 'This should appear on STDOUT' AS msg" +
                "  FROM eapBuilds ORDER BY deployDur LIMIT 3"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }

    @Test
    fun combineInputFile(testInfo: TestInfo) {
        val command =
                " | --rowNumbers" +
                " | --combineInputs" +
                " | -in  | $testDataDir/eapBuilds.csv" +
                " | -out | $testOutputDir/combineInputFile.csv" +
                " | -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale," +
                "   CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower" +
                "   FROM eapBuilds ORDER BY deployDur"
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        val resultCsv = Paths.get("$testOutputDir/combineInputFile.csv").toFile()
        assertTrue(resultCsv.exists())
        val columnNames = parseColumnsFromFirstCsvLine(resultCsv)
        assertEquals(9, columnNames.size.toLong(), "Column names fit")
        assertEquals("warmupslower", columnNames[8].lowercase())
    }

    @Test
    fun combineInputDir_defaultSql(testInfo: TestInfo) {
        val command =
                " | -in  | $testDataDir/sample-multiFile-all" +
                " | --combineInputs" +
                " | -out | $testOutputDir/combineInputDir.csv"
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        val resultCsv = Paths.get("$testOutputDir/combineInputDir.csv").toFile()
        assertTrue(resultCsv.exists())
        val columnNames = parseColumnsFromFirstCsvLine(resultCsv)
        assertEquals(13, columnNames.size.toLong()) { "Column names fit: $columnNames" }
        assertIterableEquals("Op,recording_group_id,status,storage_base_url,owner_id,room_id,session_id,requested_ts,created_ts,deleted_ts,updated_ts,is_deleted,acc_consumer_id".split(","), columnNames)
    }

    /**
     * Fails because of:
     * $table placeholder not replaced when dir input with non-canonical JSONs is used. #149
     * https://github.com/OndraZizka/csv-cruncher/issues/149
     *
     * Fails in extractColumnsInfoFrom1LineSelect()
     * when querying
     * SQL: SELECT $table.* FROM $table LIMIT 1
     *
     * The solution is not trivial, it needs to determine the output columns, using some merging of common columns,
     * probably utilizing SQL's NATURAL JOIN to reuse the same-named columns.
     */
    @Test
    fun combineInputDir_JsonAndCsv_defaultSql_issue149(testInfo: TestInfo) {
        val command =
                " | -in  | $testDataDir/sample-multiFile-json+csv" +
                " | --combineInputs" +
                " | -out | $testOutputDir/combineInputDir_json+csv.csv"
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        val resultCsv = Paths.get("$testOutputDir/combineInputDir_json+csv.csv").toFile()
        assertTrue(resultCsv.exists())
        val columnNames = parseColumnsFromFirstCsvLine(resultCsv)
        assertEquals(9, columnNames.size.toLong(), "Column names fit")
        assertEquals("warmupslower", columnNames[8].lowercase())
    }

    @Test
    fun combineInputFiles_sort(testInfo: TestInfo) {
        val command = "--json=entries" +
                " | --rowNumbers" +
                " | --combineInputs=concat" +
                " | --combineDirs=all" +
                " | --sortInputFileGroups" +
                " | -in  | $testDataDir/sample-multiFile-all" +
                " | -out | $testOutputDir/combineInputFiles_sort.csv | --overwrite" +
                " | -sql | SELECT sample_multifile_all.* FROM sample_multifile_all"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        val csvFile = Paths.get("$testOutputDir/combineInputFiles_sort.csv").toFile()
        CsvCruncherTestUtils.checkThatIdsAreIncrementing(listOf(csvFile), 3, true)
    }

    @Test
    @Disabled("Not yet implemented")
    fun combine_perRootSubDir(testInfo: TestInfo) {
        val command = "--json=entries" +
                " | --rowNumbers" +
                " | --combineInputs=concat" +
                " | --combineDirs=perInputSubdir" +
                " | --exclude=.*/LOAD.*\\.csv" +
                " | -in  | $testDataDir/sample-collab/" +
                " | -out | $testOutputDir/combine_perRootSubDir.csv" +
                " | -sql | SELECT session_uid, name, session_type, created_time, modified_date" +
                "    FROM concat ORDER BY session_type, created_time DESC"
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        val resultCsv = Paths.get("$testOutputDir/combine_perRootSubDir.csv").toFile()
        assertTrue(resultCsv.exists())
        val columnNames = parseColumnsFromFirstCsvLine(resultCsv)
        assertEquals(5, columnNames.size.toLong(), "Column names fit")
        assertEquals("modified_date", columnNames[4].lowercase())

        // TODO: Add content verifications.
    }

    @Test
    fun combine_selectStar_negative(testInfo: TestInfo) {
        val command = "--json | --combineInputs | --rowNumbers" +
                " | --exclude=.*/LOAD.*\\.csv" +
                " | -in  | $testDataDir/sampleMultiFilesPerDir/session_telephony_pins/" +
                " | -out | $testOutputDir/session_telephony_pins.csv" +
                " | -sql | SELECT * FROM session_telephony_pins"

        assertThrows<CrucherConfigException> { CsvCruncherTestUtils.runCruncherWithArguments(command) }
    }

    @Test
    fun combine_selectStar_qualified(testInfo: TestInfo) {
        // cruncherCounter, Op,id,uuid,session_id,pin,pin_type,pin_access_type,enrollment_id,created_time,modified_time
        // 123456..., I,9999,950c2668-794b-4cf9-894a-af6aea5bf5d5,1000,1234567891,0,0,,2018-08-02 07:34:55.303000,2018-08-02 07:34:55.303000
        val inputCsv = "$testOutputDir/combine_selectStar_qualified.csv"
        val command = "--json | --combineInputs | --rowNumbers" +
                " | --exclude=.*/LOAD.*\\.csv" +
                " | -in  | $testDataDir/sampleMultiFilesPerDir/session_telephony_pins/" +
                " | -out | $inputCsv" +
                " | -sql | SELECT session_telephony_pins.* FROM session_telephony_pins"

        CsvCruncherTestUtils.runCruncherWithArguments(command)

        val resultCsv = Paths.get(inputCsv).toFile()
        assertTrue(resultCsv.exists())
        val columnNames = parseColumnsFromFirstCsvLine(resultCsv)
        assertEquals(10, columnNames.size.toLong(), "Column names fit")
        assertEquals("modified_time", columnNames[9].lowercase())
        val shouldBeNull = CsvCruncherTestUtils.getCsvCellValue(resultCsv, 1, 8)
        assertEquals(null, shouldBeNull, "row 1, col 9 should be null")
    }

    @Test
    fun collab_ApolloRecGroup(testInfo: TestInfo) {
        val command = "--json | --combineInputs" +
                " | --exclude=.*/LOAD.*\\.csv" +
                " | -in  | $testDataDir/sample-collab/apollo_recording_group/" +
                " | -out | $testOutputDir/apollo_recording_group.csv" +
                " | -sql | SELECT * FROM apollo_recording_group"

        CsvCruncherTestUtils.runCruncherWithArguments(command)

        // TODO: Add the verifications.
    }

    @Test
    fun collab_SessTelPins(testInfo: TestInfo) {
        // Op,id,uuid,session_id,pin,pin_type,pin_access_type,enrollment_id,created_time,modified_time
        // I,9999,950c2668-794b-4cf9-894a-af6aea5bf5d5,1000,1234567891,0,0,,2018-08-02 07:34:55.303000,2018-08-02 07:34:55.303000
        val command = "--json | --combineInputs" +
                " | --exclude=.*/LOAD.*\\.csv" +
                " | -in  | $testDataDir/sample-collab/session_telephony_pins/" +
                " | -out | $testOutputDir/session_telephony_pins.csv" +
                " | -sql | SELECT * FROM session_telephony_pins"
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        // TODO: Add the verifications.
    }

    @Test
    fun testVersion(testInfo: TestInfo) {
        val command = "-v"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }

    @Test
    fun testHelp(testInfo: TestInfo) {
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
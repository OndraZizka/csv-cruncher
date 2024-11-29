package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import cz.dynawest.csvcruncher.CsvCruncherTestUtils.testDataDir
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

class MovedFromMavenTest {

    @Test
    fun test_testCrunch_simple(testInfo: TestInfo) {
        val command = """
            | --json=entries
            | --rowNumbers
            | -in  | $testDataDir/eapBuilds.csv
            | -out | target/results/result.csv
            | -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale,
            CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower
            FROM eapBuilds ORDER BY deployDur
        """
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        // TBD: Validate the result.
    }

    @Test
    fun test_testCrunch_combineInputFile(testInfo: TestInfo) {
        val command = """
            | --json=entries
            | --rowNumbers
            | --combineInputs
            | -in  | $testDataDir/eapBuilds.csv
            | -out | target/results/result.csv
            | -sql | SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale,
                        CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower
                        FROM eapBuilds ORDER BY deployDur
        """
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        // TBD: Validate the result.
    }

    @Test
    @Disabled("No test files in apollo_session??")
    fun test_testCrunch_combineInputFiles_perRootSubDir(testInfo: TestInfo) {
        val command = """
            | --json=entries
            | --rowNumbers
            | --combineInputs=concat
            | --combineDirs=all
            | --exclude=.*/LOAD.*\.csv
            | -in  | $testDataDir/sampleMultiFilesPerDir/apollo_session/
            | -out | target/results/result.csv
            | -sql | SELECT session_uid, name, session_type, created_time, modified_date
                        FROM concat ORDER BY session_type, created_time DESC
        """
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        // TBD: Validate the result.
    }

    @Test @Suppress("UNUSED_VARIABLE") @Disabled
    fun test_testCrunch_combineInputFiles_selectStar_negative(testInfo: TestInfo) {
        val command = """
            | --json | --combineInputs | --rowNumbers
            | --exclude=.*/LOAD.*\.csv
            | -in  | $testDataDir/sampleMultiFilesPerDir/session_telephony_pins/
            | -out | target/results/session_telephony_pins.csv
            | -sql | SELECT * FROM session_telephony_pins
        """
        // Suppress output. This will fail because the input files don't match.
        // <successCodes> <code>1</code> <code>2</code> </successCodes>
    }

    @Test
    fun test_testCrunch_combineInputFiles_selectStar_qualified(testInfo: TestInfo) {
        val command = """
            | --json | --combineInputs | --rowNumbers
            | --exclude=.*/LOAD.*\.csv
            | -in  | $testDataDir/sampleMultiFilesPerDir/session_telephony_pins/
            | -out | target/results/session_telephony_pins.csv
            | -sql | SELECT session_telephony_pins.* FROM session_telephony_pins
        """
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        // TBD: Validate the result.
    }

    @Test
    fun test_testCrunch_collab_ARG(testInfo: TestInfo) {
        val command = """
            | --json | --combineInputs
            | --exclude=.*/LOAD.*\.csv
            | -in  | $testDataDir/sample-collab/apollo_recording_group/
            | -out | target/results/apollo_recording_group.csv
            | -sql | SELECT * FROM apollo_recording_group
        """
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        // TBD: Validate the result.
    }

    @Test
    fun test_testCrunch_collab_STP(testInfo: TestInfo) {
        val command = """
            | --json | --combineInputs
            | --exclude=.*/LOAD.*\.csv
            | -in  | $testDataDir/sample-collab/session_telephony_pins/
            | -out | target/results/session_telephony_pins.csv
            | -sql | SELECT * FROM session_telephony_pins
        """
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        // TBD: Validate the result.
    }

    @Test
    fun test_testVersion(testInfo: TestInfo) {
        val command = """-v"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        // TBD: Validate the result.
    }

    @Test
    fun test_testHelp(testInfo: TestInfo) {
        val command = """-h"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        // TBD: Validate the result.
    }
}
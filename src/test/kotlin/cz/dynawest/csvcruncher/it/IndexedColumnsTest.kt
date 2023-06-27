package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.Cruncher
import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import cz.dynawest.csvcruncher.Options2
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.*

class IndexedColumnsTest {

    @ParameterizedTest(name = "givenNumbers_whenOddCheck_thenVerify{0}")
    @ValueSource(strings = arrayOf(
        // ## jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale
        "jobName",
        "jobName, buildNumber",
        "jobName, buildNumber, config",
        "jobName, config",
        "jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale",
    ))
    fun testIndexedColumns(indexedColumns: String, testInfo: TestInfo) {

        val testName = testInfo.testMethod.get().name
        val testOutputDir = CsvCruncherTestUtils.testOutputDir.resolve("sample-$testName")

        val options = Options2()
        options.newImportArgument().apply {
            this.path = CsvCruncherTestUtils.testDataDir.resolve("eapBuilds.csv")
            this.indexed = indexedColumns.split(",").map { it.trim() }
        }
        options.newExportArgument().apply {
            path = testOutputDir.resolve("testResult.csv")
            sqlQuery = "SELECT ${Cruncher.SQL_TABLE_PLACEHOLDER}.* FROM ${Cruncher.SQL_TABLE_PLACEHOLDER}"
        }
        options.overwrite = true

        Cruncher(options).crunch()
    }
}
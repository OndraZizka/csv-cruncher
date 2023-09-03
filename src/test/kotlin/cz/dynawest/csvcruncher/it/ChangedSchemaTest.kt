package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.Cruncher
import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import cz.dynawest.csvcruncher.app.Options2
import cz.dynawest.csvcruncher.app.OptionsEnums
import org.junit.jupiter.api.Test
import java.util.*

class ChangedSchemaTest {

    @Test
    @Throws(Exception::class)
    fun changedSchema() {
        val testDataDir = CsvCruncherTestUtils.testDataDir.resolve("sample-changedSchema")
        val testOutputDir = CsvCruncherTestUtils.testOutputDir.resolve("sample-changedSchema")
        val options = Options2()
        options.newImportArgument().apply { this.path = testDataDir }
        options.combineDirs = OptionsEnums.CombineDirectories.COMBINE_ALL_FILES
        options.combineInputFiles = OptionsEnums.CombineInputFiles.CONCAT
        options.newExportArgument().apply {
            path = testOutputDir.resolve("testResult.csv")
            sqlQuery = "SELECT ${Cruncher.SQL_TABLE_PLACEHOLDER}.* FROM ${Cruncher.SQL_TABLE_PLACEHOLDER}"
        }
        options.overwrite = true
        options.queryPerInputSubpart = true // This is key.
        options.initialRowNumber = 1L

        Cruncher(options).crunch()
    }
}
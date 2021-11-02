package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.Cruncher
import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import cz.dynawest.csvcruncher.Options
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.*

class ChangedSchemaIT {
    @BeforeEach
    fun dropDbSchema() {
    }

    @Test
    @Throws(Exception::class)
    fun changedSchema() {
        val testDataDir = CsvCruncherTestUtils.testDataDir.resolve("sample-changedSchema")
        val testOutputDir = CsvCruncherTestUtils.testOutputDir.resolve("sample-changedSchema")
        val options = Options()
        options.inputPaths = Arrays.asList(testDataDir.toString())
        options.combineDirs = Options.CombineDirectories.COMBINE_ALL_FILES
        options.combineInputFiles = Options.CombineInputFiles.CONCAT
        options.outputPathCsv = testOutputDir.resolve("testResult.csv").toString()
        options.overwrite = true
        options.queryPerInputSubpart = true // This is key.
        options.initialRowNumber = 1L
        options.sql = "SELECT " + Cruncher.SQL_TABLE_PLACEHOLDER + ".* FROM " + Cruncher.SQL_TABLE_PLACEHOLDER

        // TODO: This test fails, because the tables created are NULL_1, NULL_2
        //      which is OK per se, but I need to implement dumping without -sql
        //      and dump all automaticaly.

        // TODO: Also let's switch to LogBack - JUL sucks.
        Cruncher(options).crunch()
    }
}
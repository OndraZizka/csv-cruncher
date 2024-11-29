package cz.dynawest.csvcruncher.app

import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import cz.dynawest.csvcruncher.CsvCruncherTestUtils.testDataDir
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.nio.file.Path

class OptionsParsingTest {

    @Test fun testOptionsParsing(testInfo: TestInfo) {

        val command = "--json | --combineInputs" +
            " | --exclude=.*/LOAD.*\\.csv" +
            " | -in  | ${CsvCruncherTestUtils.testDataDir}/sample-collab/apollo_recording_group/" +
            " | -out | target/testResults/apollo_recording_group.csv" +
            " | -sql | SELECT * FROM apollo_recording_group" +
            " | -initSql | foo/init1.sql | foo/init2.sql"

        val arguments = command.split("|").map { it.trim() }
        val options = OptionsParser.parseArgs(arguments.toTypedArray())!!

        // The default values
        Assertions.assertThat(options.jsonExportFormat).isEqualTo(OptionsEnums.JsonExportFormat.ENTRY_PER_LINE)
        Assertions.assertThat(options.combineInputFiles).isEqualTo(OptionsEnums.CombineInputFiles.CONCAT)

        // Defaults for imports/exports
        Assertions.assertThat(options.excludePathsRegex.toString()).isEqualTo(".*/LOAD.*\\.csv")

        // Per import/export
        Assertions.assertThat(options.importArguments).size().isEqualTo(1)
        Assertions.assertThat(options.importArguments).element(0).extracting{it.path}.isEqualTo(Path.of("$testDataDir/sample-collab/apollo_recording_group/"))

        Assertions.assertThat(options.exportArguments).size().isEqualTo(1)
        Assertions.assertThat(options.exportArguments).element(0).extracting{it.path}.isEqualTo(Path.of("target/testResults/apollo_recording_group.csv"))
        Assertions.assertThat(options.exportArguments).element(0).extracting{it.sqlQuery}.isEqualTo("SELECT * FROM apollo_recording_group")

        Assertions.assertThat(options.initSqlArguments).size().isEqualTo(2)
        Assertions.assertThat(options.initSqlArguments).element(0).extracting{it.path}.isEqualTo(Path.of("foo/init1.sql"))
        Assertions.assertThat(options.initSqlArguments).element(1).extracting{it.path}.isEqualTo(Path.of("foo/init2.sql"))
    }

}
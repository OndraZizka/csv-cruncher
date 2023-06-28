package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CrucherConfigException
import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

/**
 * The testdata contain a change in columns structure, so CSV Cruncher needs to be run with --queryPerInputSubpart.
 */
class QueryPerInputSubpartTest {

    var inPath: Path = Paths.get("src/test/data/sample-queryPerInputSubpart/oauth_consumer")

    @Test
    fun queryPerInputSubpart() {
        val outputDir = Paths.get("target/testResults/queryPerInputSubpart.csv")
        val command = "--json | --combineInputs | --queryPerInputSubpart | --rowNumbers" +
                " | -in  | " + inPath +  // " | --exclude=.*/LOAD.*\\.csv" +
                " | -out | " + outputDir +
                " | -sql | SELECT \$table.* FROM \$table"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        checkOutputFiles(outputDir)
        // TODO: Add more verifications.
    }

    @Test
    @Throws(Exception::class)
    fun queryPerInputSubpart_defaultSQL() {
        val outputDir = Paths.get("target/testResults/queryPerInputSubpart_defaultSQL.csv")
        val command = "--json | --combineInputs | --queryPerInputSubpart | --rowNumbers" +
                " | -in  | " + inPath +  // " | --exclude=.*/LOAD.*\\.csv" +
                " | -out | " + outputDir
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        checkOutputFiles(outputDir)
        // TODO: Add the verifications.
    }

    @Test
    @Throws(Exception::class)
    fun queryPerInputSubpart_negative() {
        val command = "--json | --combineInputs | --queryPerInputSubpart | --rowNumbers" +
                " | -in  | " + inPath +  // " | --exclude=.*/LOAD.*\\.csv" +
                " | -out | target/testResults/queryPerInputSubpart_negative.csv" +
                " | -sql | SELECT oauth_consumer.* FROM oauth_consumer"
        try {
            CsvCruncherTestUtils.runCruncherWithArguments(command)
            Assertions.fail("Should have thrown CsvCrucherConfigException, --queryPerInputSubpart needs generic SQL.")
        } catch (ex: CrucherConfigException) {
            assertTrue(ex.message!!.contains("queryPerInputSubpart"))
            assertTrue(ex.message!!.contains("\$table"))
        }
    }

    private fun checkOutputFiles(outputDir: Path) {
        assertTrue(outputDir.toFile().exists())
        assertTrue(outputDir.resolve("oauth_consumer_1.json").toFile().exists())
        assertTrue(outputDir.resolve("oauth_consumer_2.json").toFile().exists())
        val csv1 = Paths.get(outputDir.toString() + "_concat").resolve("oauth_consumer_1.csv").toFile()
        val csv2 = Paths.get(outputDir.toString() + "_concat").resolve("oauth_consumer_2.csv").toFile()
        assertTrue(csv1.exists())
        assertTrue(csv2.exists())
        val files = Arrays.asList(csv1, csv2)
        CsvCruncherTestUtils.checkThatIdsAreIncrementing(files, 2, false)
    }
}
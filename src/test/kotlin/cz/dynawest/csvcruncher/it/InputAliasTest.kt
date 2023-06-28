package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CsvCruncherException
import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.nio.file.Paths
import kotlin.io.path.deleteIfExists

class InputAliasTest {

    val outputPath = Paths.get("target/results/testInputAliasing.csv")

    @BeforeEach @AfterEach
    fun cleanup(){
        outputPath.deleteIfExists()
    }

    @Test
    fun testInputAliasing_basic() {
        val inPath = Paths.get("src/test/data/eapBuilds.csv")

        val command = """-in | $inPath | -as | foo | -out | $outputPath | -sql | SELECT * FROM foo"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }

    @Test
    fun testInputAliasing_wrongNameInSql() {
        val inPath = Paths.get("src/test/data/eapBuilds.csv")
        val outputPath = Paths.get("target/results/testInputAliasing.csv")

        assertThrows<CsvCruncherException> {
            val command = """-in | $inPath | -as | foo | -out | $outputPath | -sql | SELECT * FROM bar"""
            CsvCruncherTestUtils.runCruncherWithArguments(command)
        }
    }

    @Test @Disabled(" File names normalized to table names collide: .../data/eapBuilds.csv, .../data/eapBuilds.csv")
    fun testInputAliasing_sameInputUnderTwoAliases() {
        val inPath = Paths.get("src/test/data/eapBuilds.csv")

        val command = "-in | $inPath | -as | foo1 | -in | $inPath | -as | foo2 | -out | $outputPath | -sql | SELECT * FROM foo1 UNION SELECT * FROM foo2"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
    }

    @Test @Disabled(" File names normalized to table names collide: .../data/eapBuilds.csv, .../data/eapBuilds.csv")
    fun testInputAliasing_sameInputUnderDuplicateAliases() {
        val inPath = Paths.get("src/test/data/eapBuilds.csv")

        val command = "-in | $inPath | -as | foo1 | -in | $inPath | -as | foo1 | -out | $outputPath | -sql | SELECT * FROM foo1"

        assertThrows<Exception> {
            CsvCruncherTestUtils.runCruncherWithArguments(command)
        }
    }

}
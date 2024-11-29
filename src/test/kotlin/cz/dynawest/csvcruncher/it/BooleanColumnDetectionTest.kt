package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import cz.dynawest.csvcruncher.CsvCruncherTestUtils.testDataDir
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.*
import java.nio.file.Path
import java.nio.file.Paths
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonValue
import kotlin.io.path.deleteIfExists

/**
 * The testdata contain a change in columns structure, so CSV Cruncher needs to be run with --queryPerInputSubpart.
 */
class BooleanColumnDetectionTest {

    private var inPath: Path = Paths.get("$testDataDir/boolTable.csv")

    @Test
    @Throws(Exception::class)
    fun testBooleanColumns(testInfo: TestInfo) {
        val outputDir = Paths.get("target/testResults/testBooleanColumns.csv")
        outputDir.deleteIfExists()

        val command = "--json" +
                " | -in  | " + inPath +
                " | -out | " + outputDir +
                " | -sql | SELECT boolTable.* FROM boolTable"
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        val jsonFile = File(outputDir.toString().replaceFirst("\\.csv$".toRegex(), ".json"))
        Assertions.assertTrue(jsonFile.exists())
        verifyBooleanResults(jsonFile)
    }

    /*
     * ID, boolUpper, bookLower, boolNull, boolYesNo, boolYnLower, boolYnUpper,bool01
     * 1,TRUE,true,true,yes,y,Y,1
     * 2,FALSE,false,false,no,n,N,0
     * 3,FALSE,false,,no,n,N,0
     */
    @Throws(IOException::class)
    private fun verifyBooleanResults(csvFile: File) {
        BufferedReader(FileReader(csvFile)).use { reader ->
            verifyNextLine(reader, true)
            verifyNextLine(reader, false)
        }
    }

    @Throws(IOException::class)
    private fun verifyNextLine(reader: BufferedReader, expectedValue: Boolean) {
        val line = reader.readLine()
        //System.out.println(":: " + line);
        val jsonReader = Json.createReader(StringReader(line))
        val row = jsonReader.readObject()
        verifyPropertyIsBoolean(expectedValue, row, "boolUpper")
        verifyPropertyIsBoolean(expectedValue, row, "boolLower")
    }

    private fun verifyPropertyIsBoolean(expectedValue: Boolean, row: JsonObject, propertyName: String) {
        val expectedValue_ = if (expectedValue) JsonValue.ValueType.TRUE else JsonValue.ValueType.FALSE
        val jsonValue = row[propertyName]
        val boolUpperValType = jsonValue?.valueType
        Assertions.assertEquals(expectedValue_, boolUpperValType, "$propertyName should be a boolean, specifically $expectedValue_\nRow: $row")
    }

    @Test @Disabled
    @Throws(Exception::class)
    fun invalidCombination_noSqlWithoutPerTableQuery(testInfo: TestInfo) {
        val outputDir = Paths.get("target/testResults/testBooleanColumns.csv")
        val command = "--json" +
                " | -in  | " + inPath +  // " | --exclude=.*/LOAD.*\\.csv" +
                " | -out | " + outputDir
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        // TODO: Add  verifications.
    }
}
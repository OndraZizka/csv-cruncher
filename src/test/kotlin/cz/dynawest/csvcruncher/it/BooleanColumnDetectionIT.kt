package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import org.junit.Assert
import org.junit.Ignore
import org.junit.Test
import java.io.*
import java.nio.file.Paths
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonValue

/**
 * The testdata contain a change in columns structure, so CSV Cruncher needs to be run with --queryPerInputSubpart.
 */
class BooleanColumnDetectionIT {
    var inPath = Paths.get("src/test/data/boolTable.csv")
    @Test
    @Throws(Exception::class)
    fun testBooleanColumns() {
        val outputDir = Paths.get("target/testResults/testBooleanColumns.csv")
        val command = "--json" +
                " | -in  | " + inPath +
                " | -out | " + outputDir +
                " | -sql | SELECT boolTable.* FROM boolTable"
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        val jsonFile = File(outputDir.toString().replaceFirst("\\.csv$".toRegex(), ".json"))
        Assert.assertTrue(jsonFile.exists())
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
    private fun verifyNextLine(reader: BufferedReader, expectedValues: Boolean) {
        val line = reader.readLine()
        //System.out.println(":: " + line);
        val jsonReader = Json.createReader(StringReader(line))
        val row = jsonReader.readObject()
        verifyPropertyIsBoolean(expectedValues, row, "boolupper")
        verifyPropertyIsBoolean(expectedValues, row, "boollower")
    }

    private fun verifyPropertyIsBoolean(expectedValues: Boolean, row: JsonObject, propertyName: String) {
        val expectedValue = if (expectedValues) JsonValue.ValueType.TRUE else JsonValue.ValueType.FALSE
        val jsonValue = row[propertyName]
        val boolUpperValType = jsonValue!!.valueType
        Assert.assertEquals("$propertyName should be a boolean, specifically $expectedValue", expectedValue, boolUpperValType)
    }

    @Test
    @Ignore // Should throw Invalid...
    @Throws(Exception::class)
    fun invalidCombination_noSqlWithoutPerTableQuery() {
        val outputDir = Paths.get("target/testResults/testBooleanColumns.csv")
        val command = "--json" +
                " | -in  | " + inPath +  // " | --exclude=.*/LOAD.*\\.csv" +
                " | -out | " + outputDir
        CsvCruncherTestUtils.runCruncherWithArguments(command)
        // TODO: Add  verifications.
    }
}
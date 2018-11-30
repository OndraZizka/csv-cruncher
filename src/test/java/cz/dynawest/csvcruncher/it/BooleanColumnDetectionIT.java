package cz.dynawest.csvcruncher.it;

import cz.dynawest.csvcruncher.CsvCruncherTestUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * The testdata contain a change in columns structure, so CSV Cruncher needs to be run with --queryPerInputSubpart.
 */
public class BooleanColumnDetectionIT
{
    Path inPath = Paths.get("src/test/data/boolTable.csv");


    @Test
    public void testBooleanColumns() throws Exception
    {
        Path outputDir = Paths.get("target/testResults/testBooleanColumns.csv");

        String command = "--json" +
                " | -in  | " + inPath +
                " | -out | " + outputDir +
                " | -sql | SELECT boolTable.* FROM boolTable";

        CsvCruncherTestUtils.runCruncherWithArguments(command);

        File jsonFile = new File(outputDir.toString().replaceFirst("\\.csv$", ".json"));
        Assert.assertTrue(jsonFile.exists());
        verifyBooleanResults(jsonFile);
    }

    /*
     * ID, boolUpper, bookLower, boolNull, boolYesNo, boolYnLower, boolYnUpper,bool01
     * 1,TRUE,true,true,yes,y,Y,1
     * 2,FALSE,false,false,no,n,N,0
     * 3,FALSE,false,,no,n,N,0
     */
    private void verifyBooleanResults(File csvFile) throws IOException
    {
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFile));)
        {
            verifyNextLine(reader, true);
            verifyNextLine(reader, false);
        }
    }

    private void verifyNextLine(BufferedReader reader, boolean expectedValues) throws IOException
    {
        String line = reader.readLine();
        //System.out.println(":: " + line);
        JsonReader jsonReader = Json.createReader(new StringReader(line));
        JsonObject row = jsonReader.readObject();

        verifyPropertyIsBoolean(expectedValues, row, "boolupper");
        verifyPropertyIsBoolean(expectedValues, row, "boollower");
    }

    private void verifyPropertyIsBoolean(boolean expectedValues, JsonObject row, String propertyName)
    {
        JsonValue.ValueType expectedValue = expectedValues ? JsonValue.ValueType.TRUE : JsonValue.ValueType.FALSE;
        JsonValue jsonValue = row.get(propertyName);
        JsonValue.ValueType boolUpperValType = jsonValue.getValueType();
        Assert.assertEquals(propertyName + " should be a boolean, specifically " + expectedValue, expectedValue, boolUpperValType);
    }

    @Test
    @Ignore // Should throw Invalid...
    public void invalidCombination_noSqlWithoutPerTableQuery() throws Exception
    {
        Path outputDir = Paths.get("target/testResults/testBooleanColumns.csv");

        String command = "--json" +
                " | -in  | " + inPath +
                // " | --exclude=.*/LOAD.*\\.csv" +
                " | -out | " + outputDir;

        CsvCruncherTestUtils.runCruncherWithArguments(command);
        // TODO: Add  verifications.
    }

}

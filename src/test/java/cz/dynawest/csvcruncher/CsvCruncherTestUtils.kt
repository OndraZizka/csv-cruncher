package cz.dynawest.csvcruncher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;

public class CsvCruncherTestUtils
{
    /**
     * @return Path to the default test data dir.
     */
    public static Path getTestDataDir() {
        return Paths.get(System.getProperty("user.dir")).resolve("src/test/data/");
    }

    /**
     * @return Path to the default test output dir.
     */
    public static Path getTestOutputDir() {
        return Paths.get(System.getProperty("user.dir")).resolve("target/testResults/");
    }


    /**
     * Runs CSV Cruncher with the guven command, which is | separated arguments.
     */
    public static void runCruncherWithArguments(String command) throws Exception
    {
        List<String> collect = Arrays.stream(command.split("\\|")).map(String::trim).filter(x -> !x.isEmpty()).collect(Collectors.toList());
        String[] args = collect.toArray(new String[0]);

        App.mainNoExit(args);
    }

    /**
     * Reads the given CSV file, skips the first line, and then checks if the 2nd column is an incrementing number
     * (just like in the input files).
     *
     * @param columnOffset1Based The 1-based column offset to check the values from. Does not parse quotes, just splits by commas.
     * @param successive Whether the checked values must increment by one, or whether they must just grow.
     */
    public static void checkThatIdsAreIncrementing(List<File> csvFiles, int columnOffset1Based, boolean successive)
    {
        Integer previousId = null;
        for (File csvFile : csvFiles)
        {
            try (BufferedReader reader = new BufferedReader(new FileReader(csvFile));)
            {
                reader.readLine();

                String line;
                while (null != (line = reader.readLine())) {
                    //System.out.println(":: " + line);
                    String[] values = StringUtils.splitPreserveAllTokens(line, ",");
                    String idStr = values[columnOffset1Based - 1];
                    int id = Integer.parseInt(idStr);
                    if (previousId != null && "I".equals(values[0])) {
                        String msgT = successive ? "prevId %d +1 = %d in %s" : "prevId %d < %d in %s";
                        String msg = String.format(msgT, previousId, id, csvFile.getPath());
                        if (successive)
                            Assert.assertEquals(msg, previousId + 1, id);
                        else
                            Assert.assertTrue(msg, previousId < id);
                    }
                    previousId = id;
                }
            }
            catch (Exception ex) {
                throw new RuntimeException("Unexpected error parsing the CSV: " + ex.getMessage(), ex);
            }
        }
    }

    /**
     * Reads one cell from a CSV file. Returns the value as a string, or null if it is an empty string.
     * @param csvFile
     * @param lineOffset1Based    1 for 1st line, etc.
     * @param columnOffset0Based  0 for 1st column, etc.
     */
    public static String getCsvCellValue(File csvFile, int lineOffset1Based, int columnOffset0Based)
    {
        if (lineOffset1Based < 1)
            throw new IllegalArgumentException("lineOffset1Based must be >= 1.");

        try {
            Reader reader = new FileReader(csvFile);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withNullString("")
                    .parse(reader);

            CSVRecord csvRecord = null;
            Iterator<CSVRecord> iterator = records.iterator();
            for (int i = 0; i < lineOffset1Based; i++) {
                if (!iterator.hasNext()) {
                    String msg = String.format("Looked for %dth line, found only %d in %s", lineOffset1Based, i, csvFile);
                    throw new RuntimeException(msg);
                }
                csvRecord = iterator.next();
            }
            if (csvRecord.size() <= columnOffset0Based) {
                String msg = String.format("Too few columns, looking for %dth, found %d in %s", columnOffset0Based, csvRecord.size(), csvFile);
                throw new RuntimeException(msg);
            }
            return csvRecord.get(columnOffset0Based);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

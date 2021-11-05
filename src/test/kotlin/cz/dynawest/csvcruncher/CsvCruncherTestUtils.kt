package cz.dynawest.csvcruncher

import cz.dynawest.csvcruncher.app.App
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVRecord
import org.apache.commons.lang3.StringUtils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import java.io.*
import java.nio.file.Path
import java.nio.file.Paths

object CsvCruncherTestUtils {
    /**
     * @return Path to the default test data dir.
     */
    val testDataDir: Path
        get() = Paths.get(System.getProperty("user.dir")).resolve("src/test/data/")

    /**
     * @return Path to the default test output dir.
     */
    val testOutputDir: Path
        get() = Paths.get(System.getProperty("user.dir")).resolve("target/testResults/")

    /**
     * Runs CSV Cruncher with the guven command, which is | separated arguments.
     */
    @Throws(Exception::class)
    fun runCruncherWithArguments(command: String) {
        val arguments = command
                .splitToSequence("|")
                .map { obj: String -> obj.trim { it <= ' ' } }
                .filter { x: String -> !x.isEmpty() }
                .toList().toTypedArray()
        App.mainNoExit(arguments)
    }

    /**
     * Reads the given CSV file, skips the first line, and then checks if the 2nd column is an incrementing number
     * (just like in the input files).
     *
     * @param columnOffset1Based The 1-based column offset to check the values from. Does not parse quotes, just splits by commas.
     * @param successive Whether the checked values must increment by one, or whether they must just grow.
     */
    fun checkThatIdsAreIncrementing(csvFiles: List<File>, columnOffset1Based: Int, successive: Boolean) {
        var previousId: Int? = null
        for (csvFile in csvFiles) {
            try {
                BufferedReader(FileReader(csvFile)).use { reader ->
                    reader.readLine()
                    var line: String?
                    while (null != reader.readLine().also { line = it }) {
                        //System.out.println(":: " + line);
                        val values = StringUtils.splitPreserveAllTokens(line, ",")
                        val idStr = values[columnOffset1Based - 1]
                        val id = idStr.toInt()
                        if (previousId != null && "I" == values[0]) {
                            val msgT = if (successive) "prevId %d +1 = %d in %s" else "prevId %d < %d in %s"
                            val msg = String.format(msgT, previousId, id, csvFile.path)
                            if (successive) assertEquals((previousId!! + 1).toLong(), id.toLong(), msg) else assertTrue(previousId!! < id, msg)
                        }
                        previousId = id
                    }
                }
            } catch (ex: Exception) {
                throw RuntimeException("Unexpected error parsing the CSV: " + ex.message, ex)
            }
        }
    }

    /**
     * Reads one cell from a CSV file. Returns the value as a string, or null if it is an empty string.
     * @param csvFile
     * @param lineOffset1Based    1 for 1st line, etc.
     * @param columnOffset0Based  0 for 1st column, etc.
     */
    fun getCsvCellValue(csvFile: File?, lineOffset1Based: Int, columnOffset0Based: Int): String? {
        require(lineOffset1Based >= 1) { "lineOffset1Based must be >= 1." }
        return try {
            val reader: Reader = FileReader(csvFile)
            val records: Iterable<CSVRecord> = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withNullString("")
                    .parse(reader)
            var csvRecord: CSVRecord? = null
            val iterator = records.iterator()
            for (i in 0 until lineOffset1Based) {
                if (!iterator.hasNext()) {
                    val msg = String.format("Looked for %dth line, found only %d in %s", lineOffset1Based, i, csvFile)
                    throw RuntimeException(msg)
                }
                csvRecord = iterator.next()
            }
            if (csvRecord!!.size() <= columnOffset0Based) {
                val msg = String.format("Too few columns, looking for %dth, found %d in %s", columnOffset0Based, csvRecord.size(), csvFile)
                throw RuntimeException(msg)
            }
            csvRecord[columnOffset0Based]
        } catch (e: FileNotFoundException) {
            throw RuntimeException(e)
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }
}
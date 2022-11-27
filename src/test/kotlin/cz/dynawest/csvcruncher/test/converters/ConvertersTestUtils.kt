package cz.dynawest.csvcruncher.test.converters

import cz.dynawest.csvcruncher.converters.CrunchProperty
import cz.dynawest.csvcruncher.converters.FileTabularizer
import cz.dynawest.csvcruncher.converters.json.JsonFileFlattener
import cz.dynawest.csvcruncher.converters.spreadsheet.RangeReference
import cz.dynawest.csvcruncher.converters.spreadsheet.RectRange
import cz.dynawest.csvcruncher.converters.spreadsheet.SpreadsheetFileTabularizer
import cz.dynawest.util.ResourceLoader
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.nio.file.Path

object ConvertersTestUtils {

    fun prepareEntriesFromFile(testFilePath: String, itemsArraySprout: String = "/", converter: FileTabularizer): MutableList<Map<String, CrunchProperty>> {

        val inputStream: InputStream =
            if (File(testFilePath).exists())
                FileInputStream(testFilePath)
            else
                ResourceLoader.openResourceAtRelativePath(Path.of(testFilePath))

        val testEntryProcessor = TestEntryProcessor()
        when (converter) {
            is JsonFileFlattener -> converter.visitEntries(inputStream, Path.of(itemsArraySprout), testEntryProcessor)
            is SpreadsheetFileTabularizer -> {
                val ctx = SpreadsheetFileTabularizer.ConvertingContext(
                    givenRange = RangeReference(inputPath = Path.of(testFilePath)),
                    actualBoundingRange = RectRange(cols = 1..6, rows = 1..6),
                    colNames = mapOf(1 to "Column 1", 2 to "Column 2"),
                )
                converter.visitEntries(ctx, testEntryProcessor, inputStream)
            }
        }

        return testEntryProcessor.entries
    }

}
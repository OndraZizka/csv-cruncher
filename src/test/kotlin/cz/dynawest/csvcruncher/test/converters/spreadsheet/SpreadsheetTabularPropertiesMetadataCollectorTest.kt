package cz.dynawest.csvcruncher.test.converters.spreadsheet

import cz.dynawest.csvcruncher.converters.EntriesNotFoundAtLocationException
import cz.dynawest.csvcruncher.converters.FileTabularizer
import cz.dynawest.csvcruncher.converters.PropertyInfo
import cz.dynawest.csvcruncher.converters.TabularPropertiesMetadataCollector
import cz.dynawest.csvcruncher.converters.spreadsheet.RangeReference
import cz.dynawest.csvcruncher.converters.spreadsheet.RectRange
import cz.dynawest.csvcruncher.converters.spreadsheet.SpreadsheetFileTabularizer
import cz.dynawest.util.ResourceLoader
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.FileInputStream
import java.io.InputStream
import java.nio.file.Path

class SpreadsheetTabularPropertiesMetadataCollectorTest {

    @Test @Disabled
    fun testConvertSpreadsheet_stringAndNumAtA0_noHeader() {
        val collectedProperties = collectProperties("data/fromOffice365.xlsx", tabularizer = SpreadsheetFileTabularizer())

        assertThat(collectedProperties).size().isEqualTo(2)
        assertThat(collectedProperties).containsKeys("id", "name")
        assertThat(collectedProperties.get("id")!!.types.string).isEqualTo(3)
        assertThat(collectedProperties.get("name")!!.types.string).isEqualTo(3)
    }

    @Test @Disabled
    fun testConvertSpreadsheet_stringAndNumAtA0_noHeader_sheet2() {
        val collectedProperties = collectProperties("data/fromOffice365.xlsx", "Sheet2!A0:B3", SpreadsheetFileTabularizer())
    }

    @Test fun testConvertSpreadsheet_sheetNotFound() {
        assertThrows<EntriesNotFoundAtLocationException> {
            collectProperties("data/fromOffice365.xlsx", "nonexistentSheet!A0:B1", SpreadsheetFileTabularizer())
        }
    }

    @Test fun testConvertSpreadsheet_rangeNotFound() {
        assertThrows<EntriesNotFoundAtLocationException> {
            collectProperties("data/fromOffice365.xlsx", "Y100:Z110", SpreadsheetFileTabularizer())
        }
    }


    @Test @Disabled
    fun testConvertSpreadsheet_03_A0_stringNum_header() {
        val collectedProperties = collectProperties("data/fromOffice365.xlsx", "", SpreadsheetFileTabularizer())
    }

    @Test @Disabled
    fun testConvertSpreadsheet_rangeB2C4() {
        val collectedProperties = collectProperties("data/fromOffice365.xlsx", "B2:C4", SpreadsheetFileTabularizer())
    }

    @Test @Disabled
    fun testConvertSpreadsheet_04_arrayAtRoot_differentTypes() {
        val collectedProperties = collectProperties("data/fromOffice365.xlsx", tabularizer = SpreadsheetFileTabularizer())

        assertThat(collectedProperties).size().isEqualTo(2)
        assertThat(collectedProperties).containsKeys("id", "age")
        assertThat(collectedProperties.get("id")!!.types.string).isEqualTo(5)
        assertThat(collectedProperties.get("age")!!.types.string).isEqualTo(1)
        assertThat(collectedProperties.get("age")!!.types.number).isEqualTo(1)
        assertThat(collectedProperties.get("age")!!.types.boolean).isEqualTo(1)
        assertThat(collectedProperties.get("age")!!.types.nill).isEqualTo(1)
    }


    // TBD: Backport to json.TabularPropertiesMetadataCollectorTest
    private fun collectProperties(testFilePath: String, rangeReference: String = "", tabularizer: FileTabularizer): MutableMap<String, PropertyInfo> {
        val inputStream: InputStream =
            if (testFilePath.startsWith("#"))
                javaClass.classLoader.getResourceAsStream(testFilePath)!!
            else if (testFilePath.startsWith("./"))
                FileInputStream(testFilePath)
            else
                ResourceLoader.openResourceAtRelativePath(Path.of(testFilePath))

        val collector = TabularPropertiesMetadataCollector()
        when (tabularizer) {
            //is SpreadsheetFileTabularizer -> tabularizer.visitEntries(inputStream, Path.of(itemsArraySprout), tabularPropertiesMetadataCollector)
            is SpreadsheetFileTabularizer -> {
                val ctx = SpreadsheetFileTabularizer.ConvertingContext(
                    givenRange = RangeReference.parse(rangeReference).copy(inputPath = Path.of(testFilePath)),
                    actualBoundingRange = RectRange(cols = 1..6, rows = 1..6), // TBD - get from the doc?
                    colNames = emptyMap(),
                )
                tabularizer.visitEntries(ctx, collector, inputStream)
            }
        }

        return collector.propertiesSoFar
    }

}

class ItemsArraySproutNotFound : EntriesNotFoundAtLocationException {
    constructor(location: Path) : super("Entries not found in spreadsheet at location $location.")
}

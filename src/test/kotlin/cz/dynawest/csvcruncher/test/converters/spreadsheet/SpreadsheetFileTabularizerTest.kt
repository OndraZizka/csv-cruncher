package cz.dynawest.csvcruncher.test.converters.spreadsheet

import cz.dynawest.csvcruncher.converters.CrunchProperty
import cz.dynawest.csvcruncher.converters.spreadsheet.SpreadsheetFileTabularizer
import cz.dynawest.csvcruncher.test.converters.ConvertersTestUtils.prepareEntriesFromFile
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

class SpreadsheetFileTabularizerTest {

    @Test
    fun testConvertSpreadsheet_01_basic() {

        val entries = prepareEntriesFromFile("spreadsheet/data/Excel2007-savedByLibreOfficeCalc.xlsx", "", SpreadsheetFileTabularizer())

        Assertions.assertThat(entries).size().isEqualTo(3)
    }

    @Test
    fun testConvertSpreadsheet_02_fromOffice365_xlsx() {

        val entries = prepareEntriesFromFile("spreadsheet/data/fromOffice365.xlsx", "", SpreadsheetFileTabularizer())

        Assertions.assertThat(entries).size().isEqualTo(13)
        Assertions.assertThat(entries).element(1).extracting({it["Column 1"]}).isEqualTo(CrunchProperty.String("Column 1", "Percent Over/Under to Flag"))
        Assertions.assertThat(entries).element(1).extracting({it["Column 2"]}).isEqualTo(CrunchProperty.Number("Column 2", 0.25))
        Assertions.assertThat(entries).element(2).extracting({it.size}).isEqualTo(0)
        Assertions.assertThat(entries).element(3).extracting({it.size}).isEqualTo(14)
        Assertions.assertThat(entries).element(3).extracting({it["D"]}).isEqualTo(CrunchProperty.String("D", "Estimated\nStart"))
    }

}
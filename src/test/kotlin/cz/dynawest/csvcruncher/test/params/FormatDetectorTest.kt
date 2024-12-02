package cz.dynawest.csvcruncher.test.params

import cz.dynawest.csvcruncher.app.DataFormat
import cz.dynawest.csvcruncher.app.csvRegexParts
import org.assertj.core.api.SoftAssertions
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FormatDetectorTest {

    @Test
    fun testDataFormat() {
        SoftAssertions.assertSoftly {
            for (content in listOf(
                "{", " {", "{ ", "{\n", "\n{", """{"foo":\n""", """{"foo": [\n""",
                "[", " [", " [ \n", "[ {", " [ { \"foo\": {",
                """{
                      "name": "params",
                      "version": "1.0.0",
                      "description": "",
                      "main": "index.js",
                      "scripts": {
                        "test": "echo \"Error: no test specified\" && exit 1"
                      },
                      "repository": {
                        "type": "git",
                        "url": "https://github.com/OndraZizka/csv-cruncher.git"
                      },
                      "private": true
                    }
                """.replace("\n", "")
            )) {
                assertEquals(DataFormat.JSON, DataFormat.detectFormat(content), "Should have match CSV using regex${DataFormat.JSON.beginningLineRegex.pattern} : '$content'")
            }
        }

        var colName = "col 1"
        assertTrue( colName.matches(csvRegexParts.columnName.toRegex()) )
        assertTrue( colName.matches(csvRegexParts.quotedOrUnquotedSpaced.toRegex()) )
        assertTrue( colName.matches(csvRegexParts.columnNames.toRegex()) )

        colName = " col 1 "
        assertTrue( colName.matches(csvRegexParts.quotedOrUnquotedSpaced.toRegex()) )
        assertTrue( colName.matches(csvRegexParts.columnNames.toRegex()) )

        colName = "col1,"
        assertTrue( colName.matches(csvRegexParts.columnNames.toRegex()) )

        colName = "#col 1"
        assertTrue( colName.matches(csvRegexParts.columnNames.toRegex()) )

        SoftAssertions.assertSoftly {
            for (content in listOf(
                "col1", " col1", "col1 ", "col1\n",
                "col1,", " col1,", "col1, ", "col1,\n",
                "#col1", "# col1", " #col1", "#col1\n",
                "#col1,", "# col1,", " #col1,", "#col1,\n",
                "col-1", " col_1,", " col.1,", "col 1,\n",
                "\"Column 1\", \"Column 2\", \"Column 3\", ",
                "Column 1, Column 2, Column 3",
            )) {
                assertEquals(DataFormat.CSV, DataFormat.detectFormat(content),
                    "Should have match CSV using regex${DataFormat.CSV.beginningLineRegex.pattern} : '$content'"
                )
            }
        }
    }

}
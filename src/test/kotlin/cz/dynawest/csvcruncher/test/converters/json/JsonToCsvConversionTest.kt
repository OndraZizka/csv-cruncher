package cz.dynawest.csvcruncher.test.converters.json

import cz.dynawest.csvcruncher.converters.CsvExporter
import cz.dynawest.csvcruncher.converters.TabularPropertiesMetadataCollector
import cz.dynawest.csvcruncher.converters.json.JsonFileFlattener
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Path
import kotlin.io.path.isRegularFile

class JsonToCsvConversionTest {

    @Test
    fun testJsonToCsvConversion(testInfo: TestInfo) {
        val testInputPath = "target/testData/json/github_data.json"
        val testOutputPath = "target/testData/json/github_data_${System.currentTimeMillis()}.csv"

        if (!Path.of(testInputPath).isRegularFile())
            return // It's ok, maybe the test is not run through Maven, so it was not unzipped.

        // TBD - move this to some use case class.

        val flattener = JsonFileFlattener()
        val collectedProperties = FileInputStream(testInputPath).use { inputStream ->
            // 1st pass - Put together the columns info
            val metaCollector = TabularPropertiesMetadataCollector()
            flattener.visitEntries(inputStream, Path.of("/"), metaCollector)
            metaCollector.propertiesSoFar
        }

        // 2nd pass - Fill the rest of the file as per the columns collected.
        FileInputStream(testInputPath).use { inputStream ->
            FileOutputStream(testOutputPath).use { outputStream ->
                val csvExporter = CsvExporter(outputStream, collectedProperties)
                csvExporter.beforeEntries()
                flattener.visitEntries(inputStream, Path.of("/"), csvExporter)
                csvExporter.afterEntries()
            }
        }

        assertThat(File(testOutputPath)).exists()
    }
}
package cz.dynawest.csvcruncher.test.json

import cz.dynawest.csvcruncher.converters.CsvExporter
import cz.dynawest.csvcruncher.converters.JsonFileFlattener
import cz.dynawest.csvcruncher.converters.TabularPropertiesMetadataCollector
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Path
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import kotlin.random.Random

class JsonToCsvConversionTest {

    @Test
    fun testJsonToCsvConversion() {
        val x = DateTimeFormatter.ofPattern("")
        val testInputPath = "target/testData/json/github_data.json"
        val testOutputPath = "target/testData/json/github_data_${System.currentTimeMillis()}.csv"

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
                flattener.visitEntries(inputStream, Path.of("/"), csvExporter)
            }
        }

        assertThat(File(testOutputPath)).exists()
    }
}
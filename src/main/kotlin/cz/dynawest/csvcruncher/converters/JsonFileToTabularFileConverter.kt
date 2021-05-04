package cz.dynawest.csvcruncher.converters

import java.nio.file.Path

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.TreeNode
import java.io.OutputStream
import java.io.Serializable
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.inputStream
import kotlin.io.path.name
import kotlin.io.path.outputStream


interface FileToTabularFileConverter {
    fun convert(inputPath: Path, mainArrayLocation: Path = Path.of("")): Path
}

class Entry(
    keyValues: Map<String, Serializable>
)

interface EntryProcessor {
    fun beforeEntries(entry: Entry) {}
    fun process(entry: Entry)
    fun afterEntries(entry: Entry) {}
}

@ExperimentalPathApi
class JsonFileToTabularFileConverter : FileToTabularFileConverter {

    override fun convert(inputPath: Path, mainArrayLocation: Path): Path {
        // Identify the columns
        val propertiesMetadataCollector = TabularPropertiesMetadataCollector()
        processEntries(inputPath, mainArrayLocation, propertiesMetadataCollector)

        // Cancel on too many JSON map keys
        // Optionally covert array properties?

        // Export to CSV
        val outputPath = deriveOutputPath(inputPath)
        outputPath.outputStream().use { outputStream ->
            val csvExporter = CsvExporter(outputStream, propertiesMetadataCollector)
            processEntries(inputPath, mainArrayLocation, csvExporter)
        }
        return outputPath
    }

    private fun deriveOutputPath(inputPath: Path): Path {
        val baseName = inputPath.fileName ?: "output"
        return inputPath.parent.resolve("$baseName.csv")
    }

    @ExperimentalPathApi
    fun processEntries(inputPath: Path, mainArrayLocation: Path, entryProcessor: EntryProcessor) {
        inputPath.inputStream().use { iss ->
            JsonFactory().createParser(iss).use { jsonParser: JsonParser ->
                // Find the main array with items - TODO
                walkThroughToTheCollectionOfMainItems(jsonParser, mainArrayLocation)

                // Expect an array of objects -> rows
                // TODO: Or expect object of objects -> then the property name is a first column, and the objects props the further columns
                if (jsonParser.nextToken() !== com.fasterxml.jackson.core.JsonToken.START_ARRAY) {
                    throw IllegalStateException("Expected content to be an array")
                }

                // Iterate over the tokens until the end of the array
                while (jsonParser.nextToken() !== com.fasterxml.jackson.core.JsonToken.END_ARRAY) {

                    readObjectAndPassKeyValues(jsonParser, entryProcessor)
                }
            }
        }
    }

    @ExperimentalPathApi
    private fun walkThroughToTheCollectionOfMainItems(jsonParser: JsonParser, mainArrayLocation: Path) {
        for (nextStep in mainArrayLocation) {
            val nextFieldName = jsonParser.nextFieldName()
            if (nextFieldName != nextStep.name) {
                jsonParser.readValueAsTree<TreeNode>()
                continue
            }

            jsonParser.nextToken()
        }
    }

    fun readObjectAndPassKeyValues(jsonParser: JsonParser, entryProcessor: EntryProcessor) {
    }

}

interface KeyValueWriter {
    fun writeKeyValue(keyValues: Map<String, Serializable>)
}

class CsvExporter(
        outputStream: OutputStream,
        propertiesMetadataCollector: TabularPropertiesMetadataCollector
) : EntryProcessor {
    override fun beforeEntries(entry: Entry) {
        TODO("Write the column headers.")
    }
    override fun process(entry: Entry) {
        TODO("Not yet implemented")
    }
}

class TabularPropertiesMetadataCollector : EntryProcessor {
    val propertiesSoFar: Map<String, PropertyInfo> = hashMapOf()
    override fun process(entry: Entry) {
        TODO("Not yet implemented")
    }
}

class PropertyInfo(
    val name: String
) {
    val types: TypesCount = TypesCount()
}

class TypesCount {
    var string: Int = 0
    var number: Int = 0
    var datetime: Int = 0
    var obj: Int = 0
    var array: Int = 0
}

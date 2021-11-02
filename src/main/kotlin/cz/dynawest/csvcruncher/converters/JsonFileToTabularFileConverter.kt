package cz.dynawest.csvcruncher.converters

import java.nio.file.Path

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.TreeNode
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeType
import java.io.InputStream
import java.io.OutputStream
import kotlin.io.path.inputStream
import kotlin.io.path.name
import kotlin.io.path.outputStream



class JsonFileToTabularFileConverter : FileToTabularFileConverter {

    override fun convert(inputPath: Path, mainArrayLocation: String): Path {

        val mainArrayPath = Path.of(mainArrayLocation)

        // 1st pass - Identify the columns
        val propertiesMetadataCollector = TabularPropertiesMetadataCollector()
        processEntries(inputPath, mainArrayPath, propertiesMetadataCollector)

        // Cancel on too many JSON map keys
        // Optionally covert array properties?

        // 2nd pass - Export to CSV
        val outputPath = deriveOutputPath(inputPath)
        outputPath.outputStream().use { outputStream ->
            val csvExporter = CsvExporter(outputStream, propertiesMetadataCollector)
            processEntries(inputPath, mainArrayPath, csvExporter)
        }
        return outputPath
    }

    private fun deriveOutputPath(inputPath: Path): Path {
        val baseName = inputPath.fileName ?: "output"
        return inputPath.parent.resolve("$baseName.csv")
    }

    fun processEntries(inputPath: Path, mainArrayLocation: Path, entryProcessor: EntryProcessor) {
        inputPath.inputStream().use { inputStream ->
            processEntries(inputStream, mainArrayLocation, entryProcessor)
        }
    }

    fun processEntries(inputStream: InputStream, mainArrayLocation: Path, entryProcessor: EntryProcessor) {
        val mapper = ObjectMapper()
        JsonFactory().setCodec(mapper).createParser(inputStream).use { jsonParser: JsonParser ->
            // Find the main array with items
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

    /** This deals with individual objects to be flattened into rows. */
    fun readObjectAndPassKeyValues(jsonParser: JsonParser, entryProcessor: EntryProcessor) {
        val node: JsonNode = jsonParser.readValueAsTree()
        val flatEntry = flattenNode(node, FlatteningContext())
        entryProcessor.collectPropertiesMetadata(flatEntry)
    }

    private fun flattenNode(node: JsonNode, flatteningContext: FlatteningContext): FlattenedEntrySequence {
        val fieldsFlattened: Sequence<MyProperty> = node.fields().asSequence().flatMap {
                (fieldName, value) ->
            val fullPropertyName = flatteningContext.currentPrefix + fieldName
            when (value.nodeType) {
                JsonNodeType.STRING -> sequenceOf( MyProperty.StringMyProperty(fullPropertyName, value.textValue()) )
                JsonNodeType.NUMBER -> sequenceOf( MyProperty.NumberMyProperty(fullPropertyName, value.numberValue()) )
                JsonNodeType.BOOLEAN -> sequenceOf( MyProperty.BooleanMyProperty(fullPropertyName, value.booleanValue()) )
                JsonNodeType.NULL -> sequenceOf( MyProperty.NullMyProperty(fullPropertyName) )
                JsonNodeType.ARRAY -> sequenceOf( MyProperty.ArrayMyProperty(fullPropertyName, listOf()) )
                JsonNodeType.OBJECT -> {
                    val flattenedNode: FlattenedEntrySequence = flattenNode(value, flatteningContext.withPrefixAddition("${fieldName}."))

                    flattenedNode.flattenedProperties
                }
                JsonNodeType.BINARY -> throw UnsupportedOperationException("Binary JSON nodes?")
                JsonNodeType.MISSING -> throw UnsupportedOperationException("Missing JSON nodes?")
                JsonNodeType.POJO -> throw UnsupportedOperationException("POJO JSON nodes?")
                else -> sequenceOf( MyProperty.NullMyProperty(fullPropertyName) )
            }
        }
        return FlattenedEntrySequence(fieldsFlattened)
    }
}

sealed class MyProperty (open val name: String) {
    data class NumberMyProperty(override val name: String, val value: Number): MyProperty(name)
    data class StringMyProperty (override val name: String, val value: String): MyProperty(name)
    data class BooleanMyProperty (override val name: String, val value: Boolean): MyProperty(name)
    data class NullMyProperty (override val name: String): MyProperty(name)
    data class ArrayMyProperty (override val name: String, val items: List<String>): MyProperty(name)
    data class ObjectMyProperty (override val name: String, val items: Map<String, String>): MyProperty(name)
}


data class FlatteningContext (
    val currentPrefix: String = ""
) {
    fun withPrefixAddition(prefixAddition: String) = this.copy(currentPrefix = currentPrefix + prefixAddition)
}

class FlatteningContextX (
    var currentPrefix: String = ""
) {
    fun appendPrefix(addition: String) {
        currentPrefix += addition
    }

    fun shortenPrefix(removeFromEnd: String) {
        val shortened = currentPrefix.removeSuffix(removeFromEnd)
        if (shortened == currentPrefix)
            throw IllegalStateException("Tried choping '$removeFromEnd' from flattening prefix '$currentPrefix'.")
        currentPrefix = shortened
    }

    override fun toString(): String = "FlatteningContext '$currentPrefix'"
}



class CsvExporter(
        outputStream: OutputStream,
        propertiesMetadataCollector: TabularPropertiesMetadataCollector
) : EntryProcessor {
    override fun beforeEntries(entry: FlattenedEntrySequence) {
        TODO("Write the column headers.")
    }
    override fun collectPropertiesMetadata(entry: FlattenedEntrySequence) {
        TODO("Not yet implemented")
    }
}



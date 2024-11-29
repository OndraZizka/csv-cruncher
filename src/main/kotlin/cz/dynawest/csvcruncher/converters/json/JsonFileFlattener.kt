package cz.dynawest.csvcruncher.converters.json

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonLocation
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeType
import cz.dynawest.csvcruncher.converters.CrunchProperty
import cz.dynawest.csvcruncher.converters.CsvExporter
import cz.dynawest.csvcruncher.converters.EntryProcessor
import cz.dynawest.csvcruncher.converters.FileTabularizer
import cz.dynawest.csvcruncher.converters.FlattenedEntrySequence
import cz.dynawest.csvcruncher.converters.TabularPropertiesMetadataCollector
import java.io.InputStream
import java.nio.file.Path
import kotlin.io.path.inputStream
import kotlin.io.path.name
import kotlin.io.path.outputStream


class JsonFileFlattener : FileTabularizer {

    override fun convert(inputPath: Path, mainArrayLocation: String): Path {

        val mainArrayPath = Path.of(mainArrayLocation)

        // 1st pass - Identify the columns
        val propertiesMetadataCollector = TabularPropertiesMetadataCollector()
        visitEntries(inputPath, mainArrayPath, propertiesMetadataCollector)
        // TBD: Cancel on too many JSON map keys

        // 2nd pass - Export. The exporter should be a member or a parameter.
        val outputPath = deriveOutputPath(inputPath)
        outputPath.outputStream().use { outputStream ->
            val csvExporter = CsvExporter(outputStream, propertiesMetadataCollector.propertiesSoFar)
            csvExporter.beforeEntries()
            visitEntries(inputPath, mainArrayPath, csvExporter)
            csvExporter.afterEntries()
            outputStream.flush()
        }
        return outputPath
    }

    private fun deriveOutputPath(inputPath: Path): Path {
        val baseName = inputPath.fileName ?: "output"
        return inputPath.resolveSibling("$baseName.csv")
    }

    fun visitEntries(inputPath: Path, mainArrayLocation: Path, entryProcessor: EntryProcessor) {
        inputPath.inputStream().use { inputStream ->
            visitEntries(inputStream, mainArrayLocation, entryProcessor)
        }
    }

    fun visitEntries(inputStream: InputStream, sproutPath: Path, entryProcessor: EntryProcessor) {
        val mapper = ObjectMapper()
        JsonFactory().setCodec(mapper).createParser(inputStream).use { jsonParser: JsonParser ->
            // Find the main array with items
            walkThroughToTheCollectionOfMainItems(jsonParser, sproutPath)

            // Expect an array of objects -> rows
            // TODO: Or expect map of objects -> then the property name is a first column, and the objects props the further columns
            val locationBefore = jsonParser.currentLocation()
            val nextToken = jsonParser.nextToken()
            if (nextToken !== JsonToken.START_ARRAY) {
                throw ItemsArraySproutNotFound("Items JSON Array not found after traversing over path '$sproutPath', found: $nextToken at $locationBefore")
            }

            // Iterate over the tokens until the end of the array
            while (jsonParser.nextToken() !== JsonToken.END_ARRAY) {
                readObjectAndPassKeyValues(jsonParser, entryProcessor)
            }
        }
    }

    private fun walkThroughToTheCollectionOfMainItems(jsonParser: JsonParser, itemsArrayPath: Path) {
        for (nextStep in itemsArrayPath) {
            val nextToken = jsonParser.nextToken()
            if (nextToken != JsonToken.START_OBJECT)
                throw ItemsArraySproutNotFound(itemsArrayPath, jsonParser.currentLocation())

            do {
                val nextFieldName = jsonParser.nextFieldName()
                    ?: throw ItemsArraySproutNotFound(itemsArrayPath, jsonParser.currentLocation())

                if (nextFieldName != nextStep.name) {
                    jsonParser.skipChildren()
                    jsonParser.nextValue()
                    jsonParser.skipChildren()
                    continue
                }
                else break
            } while (true)
        }
        // Now we should be right before the START_ARRAY.
    }

    /** This deals with individual objects to be flattened into rows. */
    fun readObjectAndPassKeyValues(jsonParser: JsonParser, entryProcessor: EntryProcessor) {
        val node: JsonNode = jsonParser.readValueAsTree()
        val flatEntry = flattenNode(node, TreeFlatteningContext())
        entryProcessor.processEntry(flatEntry)
    }

    private fun flattenNode(node: JsonNode, flatteningContext: TreeFlatteningContext): FlattenedEntrySequence {
        val fieldsFlattened: Sequence<CrunchProperty> = node.fields().asSequence().flatMap {
                (fieldName, value) ->
            val fullPropertyName = flatteningContext.currentPrefix + fieldName
            when (value.nodeType) {
                JsonNodeType.STRING -> sequenceOf(CrunchProperty.String(fullPropertyName, value.textValue()))
                JsonNodeType.NUMBER -> sequenceOf(CrunchProperty.Number(fullPropertyName, value.numberValue()))
                JsonNodeType.BOOLEAN -> sequenceOf(CrunchProperty.Boolean(fullPropertyName, value.booleanValue()))
                JsonNodeType.NULL -> sequenceOf(CrunchProperty.Null(fullPropertyName))
                JsonNodeType.ARRAY -> sequenceOf(CrunchProperty.Array(fullPropertyName, listOf()))
                JsonNodeType.OBJECT -> {
                    val flattenedNode: FlattenedEntrySequence = flattenNode(value, flatteningContext.withPrefixAddition("${fieldName}."))

                    flattenedNode.flattenedProperties
                }
                JsonNodeType.BINARY -> throw UnsupportedOperationException("Binary JSON nodes?")
                JsonNodeType.MISSING -> throw UnsupportedOperationException("Missing JSON nodes?")
                JsonNodeType.POJO -> throw UnsupportedOperationException("POJO JSON nodes?")
                else -> sequenceOf(CrunchProperty.Null(fullPropertyName))
            }
        }
        return FlattenedEntrySequence(fieldsFlattened)
    }
}


/**
 * This is done immutable because of how the object is passed around between lambdas.
 */
data class TreeFlatteningContext (
    val currentPrefix: String = ""
) {
    fun withPrefixAddition(prefixAddition: String) = this.copy(currentPrefix = currentPrefix + prefixAddition)
}


class ItemsArraySproutNotFound : Exception {
    constructor(sproutPath: Path, location: JsonLocation) : super("Items JSON Array not found after traversing over path '$sproutPath', Not matching at $location.")
    constructor(msg: String) : super(msg)
}



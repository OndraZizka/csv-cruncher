package cz.dynawest.csvcruncher.converters

import java.io.Serializable
import java.nio.file.Path

interface FileToTabularFileConverter {
    fun convert(inputPath: Path, mainArrayLocation: String = ""): Path
}

data class FlattenedEntrySequence(
    val flattenedProperties: Sequence<MyProperty>
) {
    fun consumeToString() = flattenedProperties.joinToString(", ")
}

interface EntryProcessor {
    fun collectPropertiesMetadata(entry: FlattenedEntrySequence)
    fun beforeEntries(entry: FlattenedEntrySequence) {}
    fun afterEntries(entry: FlattenedEntrySequence) {}
}


interface KeyValueWriter {
    fun writeKeyValue(keyValues: Map<String, Serializable>)
}

class PropertyInfo(
    val name: String
) {
    val types: TypesCount = TypesCount()
    var maxLength: Int = 0
}

class TypesCount {
    var string: Int = 0
    var number: Int = 0
    var boolean: Int = 0
    var datetime: Int = 0
    var nill: Int = 0
    var obj: Int = 0
    var array: Int = 0
}
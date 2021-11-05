package cz.dynawest.csvcruncher.converters

import java.io.Serializable
import java.nio.file.Path

/**
 * The implementations of this interface:
 *  - parse the file at the given path,
 *  - parse it for data entries,
 *  - convert them into table-like data structure - i.e. rows with named columns,
 *  - stores this structure into another file and return the path to it.
 */
interface FileTabularizer {
    fun convert(inputPath: Path, mainArrayLocation: String = ""): Path
}

data class FlattenedEntrySequence(
    val flattenedProperties: Sequence<MyProperty>
)

interface EntryProcessor {
    fun processEntry(entry: FlattenedEntrySequence)
    fun beforeEntries() {}
    fun afterEntries() {}
}


interface KeyValueWriter {
    fun writeKeyValue(keyValues: Map<String, Serializable>)
}

data class PropertyInfo(
    val name: String
) {
    val types: TypesCount = TypesCount()
    var maxLength: Int = 0

    override fun toString() = "max $maxLength, types: $types"
}

data class TypesCount (
    var string: Int = 0,
    var number: Int = 0,
    var boolean: Int = 0,
    var datetime: Int = 0,
    var nill: Int = 0,
    var obj: Int = 0,
    var array: Int = 0,
) {
    val total: Int
        get() = string + number + boolean + datetime + nill + obj + array

}

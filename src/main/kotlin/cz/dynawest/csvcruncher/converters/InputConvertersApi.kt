package cz.dynawest.csvcruncher.converters

import java.io.Serializable
import java.nio.file.Path

interface FileToTabularFileConverter {
    fun convert(inputPath: Path, mainArrayLocation: String = ""): Path
}

data class Entry(
    val keyValues: Map<String, Serializable>
)

interface EntryProcessor {
    fun process(entry: Entry)
    fun beforeEntries(entry: Entry) {}
    fun afterEntries(entry: Entry) {}
}


interface KeyValueWriter {
    fun writeKeyValue(keyValues: Map<String, Serializable>)
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

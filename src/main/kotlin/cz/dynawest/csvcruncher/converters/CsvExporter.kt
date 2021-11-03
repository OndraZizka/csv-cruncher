package cz.dynawest.csvcruncher.converters

import java.io.OutputStream
import java.time.LocalDateTime

class CsvExporter(
    val outputStream: OutputStream, // TBD: Rather have the file path and handle opening here?
    val columnsInfo: MutableMap<String, PropertyInfo>,
    val columnSeparator: String = ","
) : EntryProcessor {
    override fun beforeEntries(entry: FlattenedEntrySequence) {
        val writer = outputStream.writer()
        writer.write("## Coverted by CsvCruncher on ${LocalDateTime.now()}")

        val header = columnsInfo.map { it.value.name }.joinToString(separator = columnSeparator + " ")
        writer.write(header + "\n")
    }
    override fun processEntry(entry: FlattenedEntrySequence) {
        val entryPropsMap: Map<String, MyProperty> = entry.flattenedProperties.associateBy { it.name }
        val line = columnsInfo.map { column -> entryPropsMap.get(column.key)?.toCsvString() ?: "" }.joinToString(
            columnSeparator
        )
        outputStream.writer().write(line + "\n\n")
    }
}
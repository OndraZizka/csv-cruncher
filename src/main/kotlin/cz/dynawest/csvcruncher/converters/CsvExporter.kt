package cz.dynawest.csvcruncher.converters

import cz.dynawest.csvcruncher.util.logger
import java.io.OutputStream
import java.time.LocalDateTime

class CsvExporter(
    val outputStream: OutputStream, // TBD: Rather have the file path and handle opening here?
    val columnsInfo: MutableMap<String, PropertyInfo>,
    val columnSeparator: String = ","
)
    : EntryProcessor
{
    override fun beforeEntries() {
        val writer = outputStream.writer()
        writer.write("### Converted by CsvCruncher on ${LocalDateTime.now()}\n")

        val header = columnsInfo.map { it.value.name }.joinToString(separator = "$columnSeparator ")
        log.debug("CSV header: $header")
        writer.write(header + "\n")
        writer.flush()
    }
    override fun processEntry(entry: FlattenedEntrySequence) {
        val entryPropsMap: Map<String, CrunchProperty> = entry.flattenedProperties.associateBy { it.name }
        val line = columnsInfo.map { column -> entryPropsMap.get(column.key)?.toCsvString() ?: "" }.joinToString(
            columnSeparator
        )
        outputStream.writer().write(line + "\n")
    }

    companion object { private val log = logger() }
}
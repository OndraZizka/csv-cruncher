package cz.dynawest.csvcruncher.converters

import cz.dynawest.csvcruncher.util.logger
import java.io.OutputStream
import java.time.LocalDateTime

class CsvExporter(
    outputStream: OutputStream,
    val columnsInfo: MutableMap<String, PropertyInfo>,
    val columnSeparator: String = ","
)
    : EntryProcessor
{
    val writer = outputStream.writer()

    override fun beforeEntries() {

        //writer.write("### Converted by CsvCruncher on ${LocalDateTime.now()}\n") // HSQLDB can't skip more than 1 line.

        val header = columnsInfo.map { it.value.name }.joinToString(separator = "$columnSeparator ")
        log.debug("CSV header: $header")
        writer.write(header + "\n")
        writer.flush()
    }

    override fun afterEntries() {
        writer.close()
    }

    override fun processEntry(entry: FlattenedEntrySequence) {
        val entryPropsMap: Map<String, CrunchProperty> = entry.flattenedProperties.associateBy { it.name }
        val line = columnsInfo.map { column -> entryPropsMap.get(column.key)?.toCsvString() ?: "" }.joinToString(columnSeparator)

        writer.write(line + "\n")
        writer.flush()
    }

    companion object { private val log = logger() }
}
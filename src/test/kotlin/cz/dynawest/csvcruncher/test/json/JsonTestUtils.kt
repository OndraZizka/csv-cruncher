package cz.dynawest.csvcruncher.test.json

import cz.dynawest.csvcruncher.converters.EntryProcessor
import cz.dynawest.csvcruncher.converters.FlattenedEntrySequence
import cz.dynawest.csvcruncher.converters.JsonFileToTabularFileConverter
import cz.dynawest.csvcruncher.converters.MyProperty
import cz.dynawest.csvcruncher.util.logger
import cz.dynawest.util.ResourceLoader
import java.io.InputStream
import java.nio.file.Path

object JsonTestUtils {
    fun prepareEntriesFromFile(testFilePath: String, itemsArraySprout: String = "/"): MutableList<Map<String, MyProperty>> {
        val converter = JsonFileToTabularFileConverter()
        val inputStream: InputStream = ResourceLoader.openResourceAtRelativePath(Path.of(testFilePath))

        val entries = mutableListOf<Map<String, MyProperty>>()
        converter.processEntries(inputStream, Path.of(itemsArraySprout), object : EntryProcessor {
            override fun collectPropertiesMetadata(entry: FlattenedEntrySequence) {
                val entryMap = entry.flattenedProperties.associateBy { myProp -> myProp.name }
                entries.add(entryMap)
                log.info("Entry: ${entryMap}")
            }
        })
        return entries
    }
    private val log = logger()
}
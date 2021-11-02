package cz.dynawest.csvcruncher.test

import cz.dynawest.csvcruncher.converters.FlattenedEntrySequence
import cz.dynawest.csvcruncher.converters.EntryProcessor
import cz.dynawest.csvcruncher.converters.JsonFileToTabularFileConverter
import cz.dynawest.csvcruncher.converters.MyProperty
import cz.dynawest.csvcruncher.util.logger
import cz.dynawest.util.ResourceLoader
import org.junit.Test
import java.io.InputStream
import java.nio.file.Path

class JsonFileToTabularFileConverterTest {

    @Test fun testConvertJson() {
        val converter = JsonFileToTabularFileConverter()
        val inputStream: InputStream = ResourceLoader.openResourceAtRelativePath(Path.of("01-arrayAtRoot-sameProperties.json"))

        val entries = mutableListOf<Map<String, MyProperty>>()
        converter.processEntries(inputStream, Path.of("/"), object : EntryProcessor {
            override fun collectPropertiesMetadata(entry: FlattenedEntrySequence) {
                val entryMap = entry.flattenedProperties.associateBy { myProp -> myProp.name }
                entries.add(entryMap)
                log.info("Entry: ${entryMap}")
            }
        })
    }

    companion object { private val log = logger() }
}
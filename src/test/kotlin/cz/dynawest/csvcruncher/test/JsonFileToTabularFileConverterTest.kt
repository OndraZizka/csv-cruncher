package cz.dynawest.csvcruncher.test

import cz.dynawest.csvcruncher.converters.FlattenedEntrySequence
import cz.dynawest.csvcruncher.converters.EntryProcessor
import cz.dynawest.csvcruncher.converters.JsonFileToTabularFileConverter
import cz.dynawest.csvcruncher.util.logger
import cz.dynawest.util.ResourceLoader
import org.junit.Test
import java.io.InputStream
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi

@ExperimentalPathApi
class JsonFileToTabularFileConverterTest {

    @Test fun testConvertJson() {
        val converter = JsonFileToTabularFileConverter()
        val inputStream: InputStream = ResourceLoader.openResourceAtRelativePath(Path.of("sample.json"))
        converter.processEntries(inputStream, Path.of("/"), object : EntryProcessor {
            override fun collectPropertiesMetadata(entry: FlattenedEntrySequence) {
                log.info("Entry: ${entry.consumeToString()}")
            }
        })
    }

    companion object { private val log = logger() }
}
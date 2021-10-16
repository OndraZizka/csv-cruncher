package cz.dynawest.csvcruncher.test

import cz.dynawest.csvcruncher.converters.Entry
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
            override fun process(entry: Entry) {
                log.info("Entry: $entry")
            }
        })
    }

    companion object { private val log = logger() }
}
package cz.dynawest.csvcruncher.test

import cz.dynawest.csvcruncher.converters.FlattenedEntrySequence
import cz.dynawest.csvcruncher.converters.EntryProcessor
import cz.dynawest.csvcruncher.converters.JsonFileToTabularFileConverter
import cz.dynawest.csvcruncher.converters.MyProperty
import cz.dynawest.csvcruncher.util.logger
import cz.dynawest.util.ResourceLoader
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.io.InputStream
import java.nio.file.Path

class JsonFileToTabularFileConverterTest {

    @Test
    fun testConvertJson_01_arrayAtRoot_sameProperties() {
        val entries = prepareEntriesFromFile("01-arrayAtRoot-sameProperties.json")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(MyProperty.StringMyProperty("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["name"]}).isEqualTo(MyProperty.StringMyProperty("name", "Ada"))
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(MyProperty.StringMyProperty("id", "2"))
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(MyProperty.StringMyProperty("id", "3"))
    }

    @Test
    fun `testConvertJson_02-arrayAtRoot-propertiesMissing`() {
        val entries = prepareEntriesFromFile("02-arrayAtRoot-propertiesMissing.json")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(MyProperty.StringMyProperty("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["name"]}).isNull()
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(MyProperty.StringMyProperty("id", "2"))
        Assertions.assertThat(entries).element(1).extracting({it["name"]}).isEqualTo(MyProperty.StringMyProperty("name", "Ondra"))
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(MyProperty.StringMyProperty("id", "3"))
        Assertions.assertThat(entries).element(2).extracting({it["born"]}).isEqualTo(MyProperty.StringMyProperty("born", "2016"))
    }

    @Test
    fun `testConvertJson_03-arrayAtRoot-varyingProperties`() {
        val entries = prepareEntriesFromFile("03-arrayAtRoot-varyingProperties.json")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(MyProperty.StringMyProperty("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["name"]}).isEqualTo(MyProperty.StringMyProperty("name", "Ada"))
        Assertions.assertThat(entries).element(0).extracting({it["surname"]}).isNull()
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(MyProperty.StringMyProperty("id", "2"))
        Assertions.assertThat(entries).element(1).extracting({it["surname"]}).isEqualTo(MyProperty.StringMyProperty("surname", "Zizka"))
        Assertions.assertThat(entries).element(1).extracting({it["name"]}).isNull()
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(MyProperty.StringMyProperty("id", "3"))
        Assertions.assertThat(entries).element(2).extracting({it["name"]}).isEqualTo(MyProperty.StringMyProperty("name", "Ondra"))
        Assertions.assertThat(entries).element(2).extracting({it["surname"]}).isNull()
    }

    @Test
    fun `testConvertJson_04-arrayAtRoot-differentTypes`() {
        val entries = prepareEntriesFromFile("04-arrayAtRoot-differentTypes.json")

        Assertions.assertThat(entries).size().isEqualTo(5)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(MyProperty.StringMyProperty("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["age"]}).isEqualTo(MyProperty.StringMyProperty("age", "4"))
        Assertions.assertThat(entries).element(1).extracting({it["age"]}).isEqualTo(MyProperty.NumberMyProperty("age", 15))
        Assertions.assertThat(entries).element(2).extracting({it["age"]}).isEqualTo(MyProperty.BooleanMyProperty("age", true))
        Assertions.assertThat(entries).element(3).extracting({it["age"]}).isEqualTo(MyProperty.NullMyProperty("age"))
        Assertions.assertThat(entries).element(4).extracting({it["age"]}).isNull()
    }

    private fun prepareEntriesFromFile(testFilePath: String): MutableList<Map<String, MyProperty>> {
        val converter = JsonFileToTabularFileConverter()
        val inputStream: InputStream = ResourceLoader.openResourceAtRelativePath(Path.of(testFilePath))

        val entries = mutableListOf<Map<String, MyProperty>>()
        converter.processEntries(inputStream, Path.of("/"), object : EntryProcessor {
            override fun collectPropertiesMetadata(entry: FlattenedEntrySequence) {
                val entryMap = entry.flattenedProperties.associateBy { myProp -> myProp.name }
                entries.add(entryMap)
                log.info("Entry: ${entryMap}")
            }
        })
        return entries
    }

    companion object { private val log = logger() }
}
package cz.dynawest.csvcruncher.test.json

import cz.dynawest.csvcruncher.converters.ItemsArraySproutNotFound
import cz.dynawest.csvcruncher.converters.JsonFileToTabularFileConverter
import cz.dynawest.csvcruncher.converters.PropertyInfo
import cz.dynawest.csvcruncher.converters.TabularPropertiesMetadataCollector
import cz.dynawest.csvcruncher.util.logger
import cz.dynawest.util.ResourceLoader
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.InputStream
import java.lang.IllegalStateException
import java.nio.file.Path

class TabularPropertiesMetadataCollectorTest {

    @Test
    fun `testConvertJson_01-arrayAtRoot-nestedArray`() {
        val collectedProperties = collectProperties("data/basic/01-arrayAtRoot-sameProperties.json")

        assertThat(collectedProperties).size().isEqualTo(2)
        assertThat(collectedProperties).containsKeys("id", "name")
        assertThat(collectedProperties.get("id")!!.types.string).isEqualTo(3)
        assertThat(collectedProperties.get("name")!!.types.string).isEqualTo(3)
    }

    @Test
    fun `testConvertJson_02-arrayAtRoot-propertiesMissing`() {
        val collectedProperties = collectProperties("data/basic/02-arrayAtRoot-propertiesMissing.json")

        assertThat(collectedProperties).size().isEqualTo(3)
        assertThat(collectedProperties).containsKeys("id", "name", "born")
        assertThat(collectedProperties.get("id")!!.types.string).isEqualTo(3)
        assertThat(collectedProperties.get("name")!!.types.string).isEqualTo(2)
        assertThat(collectedProperties.get("born")!!.types.string).isEqualTo(1)
    }

    @Test
    fun `testConvertJson_03-arrayAtRoot-varyingProperties`() {
        val collectedProperties = collectProperties("data/basic/03-arrayAtRoot-varyingProperties.json")

        assertThat(collectedProperties).size().isEqualTo(3)
        assertThat(collectedProperties).containsKeys("id", "name", "surname")
        assertThat(collectedProperties.get("id")!!.types.string).isEqualTo(3)
        assertThat(collectedProperties.get("name")!!.types.string).isEqualTo(2)
        assertThat(collectedProperties.get("surname")!!.types.string).isEqualTo(1)
    }

    @Test
    fun `testConvertJson_04-arrayAtRoot-differentTypes`() {
        val collectedProperties = collectProperties("data/basic/04-arrayAtRoot-differentTypes.json", "/")

        assertThat(collectedProperties).size().isEqualTo(2)
        assertThat(collectedProperties).containsKeys("id", "age")
        assertThat(collectedProperties.get("id")!!.types.string).isEqualTo(5)
        assertThat(collectedProperties.get("age")!!.types.string).isEqualTo(1)
        assertThat(collectedProperties.get("age")!!.types.number).isEqualTo(1)
        assertThat(collectedProperties.get("age")!!.types.boolean).isEqualTo(1)
        assertThat(collectedProperties.get("age")!!.types.nill).isEqualTo(1)
    }

    @Test
    fun `testConvertJson_10-arrayAtRoot-nestedObject`() {
        val collectedProperties = collectProperties("data/basic/10-arrayAtRoot-nestedObject.json", "/")

        assertThat(collectedProperties).size().isEqualTo(4)
        assertThat(collectedProperties).containsKeys("id", "name", "address.city", "address.zip")
        assertThat(collectedProperties).doesNotContainKey("address")
        assertThat(collectedProperties.get("id")!!.types.string).isEqualTo(3)
        assertThat(collectedProperties.get("address.city")!!.types.string).isEqualTo(1)
        assertThat(collectedProperties.get("address.zip")!!.types.number).isEqualTo(1)
    }

    @Test
    fun `testConvertJson_12-arrayInObject-sameProperties`() {
        val collectedProperties = collectProperties("data/basic/12-arrayInObject-sameProperties.json", "/items")

        assertThat(collectedProperties).size().isEqualTo(2)
        assertThat(collectedProperties).containsKeys("id", "name")
        assertThat(collectedProperties).doesNotContainKey("address")
        assertThat(collectedProperties.get("id")!!.types.string).isEqualTo(3)
        assertThat(collectedProperties.get("name")!!.types.string).isEqualTo(3)
    }

    @Test
    fun `testConvertJson_12-arrayInObject-sameProperties_mainArrayNotFound`() {
        assertThrows<ItemsArraySproutNotFound> {
            collectProperties("data/basic/12-arrayInObject-sameProperties.json", "/")
        }
    }



    private fun collectProperties(testFilePath: String, itemsArraySprout: String = "/"): MutableMap<String, PropertyInfo> {
        val converter = JsonFileToTabularFileConverter()
        val inputStream: InputStream = ResourceLoader.openResourceAtRelativePath(Path.of(testFilePath))
        val tabularPropertiesMetadataCollector = TabularPropertiesMetadataCollector()
        converter.processEntries(inputStream, Path.of(itemsArraySprout), tabularPropertiesMetadataCollector)
        val collectedProperties = tabularPropertiesMetadataCollector.propertiesSoFar
        return collectedProperties
    }

    companion object { private val log = logger() }
}
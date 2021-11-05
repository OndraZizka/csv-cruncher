package cz.dynawest.csvcruncher.test.json

import cz.dynawest.csvcruncher.converters.ItemsArraySproutNotFound
import cz.dynawest.csvcruncher.converters.JsonFileFlattener
import cz.dynawest.csvcruncher.converters.PropertyInfo
import cz.dynawest.csvcruncher.converters.TabularPropertiesMetadataCollector
import cz.dynawest.csvcruncher.util.logger
import cz.dynawest.util.ResourceLoader
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.io.FileInputStream
import java.io.InputStream
import java.nio.file.Path
import kotlin.test.assertEquals

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

    @Test //@Disabled("A bug in walkThroughToTheCollectionOfMainItems()")
    fun `testConvertJson_real_youtube`() {
        val collectedProperties = collectProperties("data/real/youtube.json", "/items")

        val propsList = collectedProperties.map { "\n * $it" }.joinToString()
        assertEquals(5, collectedProperties.size, propsList)
        assertThat(collectedProperties).containsKeys("kind", "etag", "id.kind", "id.videoId", "id.channelId")
        assertThat(collectedProperties).doesNotContainKey("id")
        assertThat(collectedProperties.get("id.kind")!!.types.string).isEqualTo(3)
        assertThat(collectedProperties.get("id.videoId")!!.types.string).isEqualTo(2)
    }

    @Test
    fun `testConvertJson_large_github`() {
        val collectedProperties = collectProperties("./target/testData/json/github_data.json", "/")

        val propsList = collectedProperties.map { "\n * $it" }.joinToString()
        assertEquals(615, collectedProperties.size, propsList)
        assertThat(collectedProperties).containsKeys("id", "actor.id", "actor.login", "actor.url", "payload.release.author.id")
        assertThat(collectedProperties).doesNotContainKey("payload")
        assertThat(collectedProperties.get("repo.name")!!.types.string).isEqualTo(11351)
        assertThat(collectedProperties.get("repo.id")!!.types.number).isEqualTo(11351)
        assertThat(collectedProperties.get("repo.id")!!.maxLength).isEqualTo(8)
    }



    private fun collectProperties(testFilePath: String, itemsArraySprout: String = "/"): MutableMap<String, PropertyInfo> {
        val converter = JsonFileFlattener()
        val inputStream: InputStream =
            if (testFilePath.startsWith("#"))
                javaClass.classLoader.getResourceAsStream(testFilePath)
            else if (testFilePath.startsWith("./"))
                FileInputStream(testFilePath)
            else
                ResourceLoader.openResourceAtRelativePath(Path.of(testFilePath))

        val tabularPropertiesMetadataCollector = TabularPropertiesMetadataCollector()
        converter.visitEntries(inputStream, Path.of(itemsArraySprout), tabularPropertiesMetadataCollector)
        val collectedProperties = tabularPropertiesMetadataCollector.propertiesSoFar
        return collectedProperties
    }

    companion object { private val log = logger() }
}
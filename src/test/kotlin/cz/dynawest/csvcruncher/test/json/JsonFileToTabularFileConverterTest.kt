package cz.dynawest.csvcruncher.test.json

import cz.dynawest.csvcruncher.converters.MyProperty
import cz.dynawest.csvcruncher.test.json.JsonTestUtils.prepareEntriesFromFile
import cz.dynawest.csvcruncher.util.logger
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

class JsonFileToTabularFileConverterTest {

    @Test
    fun testConvertJson_01_arrayAtRoot_sameProperties() {
        val entries = prepareEntriesFromFile("data/basic/01-arrayAtRoot-sameProperties.json", "/")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["name"]}).isEqualTo(MyProperty.String("name", "Ada"))
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "2"))
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "3"))
    }

    @Test
    fun `testConvertJson_02-arrayAtRoot-propertiesMissing`() {
        val entries = prepareEntriesFromFile("data/basic/02-arrayAtRoot-propertiesMissing.json", "/")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["name"]}).isNull()
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "2"))
        Assertions.assertThat(entries).element(1).extracting({it["name"]}).isEqualTo(MyProperty.String("name", "Ondra"))
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "3"))
        Assertions.assertThat(entries).element(2).extracting({it["born"]}).isEqualTo(MyProperty.String("born", "2016"))
    }

    @Test
    fun `testConvertJson_03-arrayAtRoot-varyingProperties`() {
        val entries = prepareEntriesFromFile("data/basic/03-arrayAtRoot-varyingProperties.json", "/")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["name"]}).isEqualTo(MyProperty.String("name", "Ada"))
        Assertions.assertThat(entries).element(0).extracting({it["surname"]}).isNull()
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "2"))
        Assertions.assertThat(entries).element(1).extracting({it["surname"]}).isEqualTo(MyProperty.String("surname", "Zizka"))
        Assertions.assertThat(entries).element(1).extracting({it["name"]}).isNull()
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "3"))
        Assertions.assertThat(entries).element(2).extracting({it["name"]}).isEqualTo(MyProperty.String("name", "Ondra"))
        Assertions.assertThat(entries).element(2).extracting({it["surname"]}).isNull()
    }

    @Test
    fun `testConvertJson_04-arrayAtRoot-differentTypes`() {
        val entries = prepareEntriesFromFile("data/basic/04-arrayAtRoot-differentTypes.json", "/")

        Assertions.assertThat(entries).size().isEqualTo(5)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["age"]}).isEqualTo(MyProperty.String("age", "4"))
        Assertions.assertThat(entries).element(1).extracting({it["age"]}).isEqualTo(MyProperty.Number("age", 15))
        Assertions.assertThat(entries).element(2).extracting({it["age"]}).isEqualTo(MyProperty.Boolean("age", true))
        Assertions.assertThat(entries).element(3).extracting({it["age"]}).isEqualTo(MyProperty.Null("age"))
        Assertions.assertThat(entries).element(4).extracting({it["age"]}).isNull()
    }

    @Test
    fun `testConvertJson_10-arrayAtRoot-nestedObject`() {
        val entries = prepareEntriesFromFile("data/basic/10-arrayAtRoot-nestedObject.json", "/")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "1"))
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "2"))
        // {"id": "3", "name": "Ondra", "address": {"city":  "Zurich", "zip": 8001}}
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "3"))
        Assertions.assertThat(entries).element(2).extracting({it["name"]}).isEqualTo(MyProperty.String("name", "Ondra"))
        Assertions.assertThat(entries).element(2).extracting({it["address"]}).isNull()
        Assertions.assertThat(entries).element(2).extracting({it["address.city"]}).isEqualTo(MyProperty.String("address.city", "Zurich"))
        Assertions.assertThat(entries).element(2).extracting({it["address.zip"]}).isEqualTo(MyProperty.Number("address.zip", 8001))
    }


    @Test
    fun `testConvertJson_11-arrayAtRoot-nestedArray`() {
        val entries = prepareEntriesFromFile("data/basic/11-arrayAtRoot-nestedArray.json", "/")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "1"))
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "2"))
        // {"id": "3", "name": "Ondra", "addresses": [{"city":  "Zurich", "zip": 8001}]}
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "3"))
        Assertions.assertThat(entries).element(2).extracting({it["name"]}).isEqualTo(MyProperty.String("name", "Ondra"))
        Assertions.assertThat(entries).element(2).extracting({it["addresses"]}).isEqualTo(MyProperty.Array("addresses", listOf())) // TODO - currently empty.
        //Assertions.assertThat(entries).element(2).extracting({it["addresses.1.city"]}).isEqualTo(MyProperty.String("addresses.1.city", "Zurich"))
        //Assertions.assertThat(entries).element(2).extracting({it["addresses.1.zip"]}).isEqualTo(MyProperty.Number("addresses.1.zip", 8001))
    }

    @Test
    fun `testConvertJson_12-arrayInObject-sameProperties`() {
        val entries = prepareEntriesFromFile("data/basic/12-arrayInObject-sameProperties.json", "/items")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["name"]}).isEqualTo(MyProperty.String("name", "Ondra"))
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "2"))
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(MyProperty.String("id", "3"))
    }

    companion object { private val log = logger() }
}
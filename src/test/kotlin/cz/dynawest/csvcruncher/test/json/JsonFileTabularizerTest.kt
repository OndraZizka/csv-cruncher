package cz.dynawest.csvcruncher.test.json

import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import cz.dynawest.csvcruncher.CsvCruncherTestUtils.testDataDir
import cz.dynawest.csvcruncher.converters.CrunchProperty
import cz.dynawest.csvcruncher.test.json.JsonTestUtils.prepareEntriesFromFile
import cz.dynawest.csvcruncher.util.logger
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.file.Paths
import java.util.function.Consumer

class JsonFileTabularizerTest {

    @Test
    fun testConvertJson_01_arrayAtRoot_sameProperties() {
        val entries = prepareEntriesFromFile("data/basic/01-arrayAtRoot-sameProperties.json", "/")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["name"]}).isEqualTo(CrunchProperty.String("name", "Ada"))
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "2"))
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "3"))
    }

    @Test
    fun `testConvertJson_02-arrayAtRoot-propertiesMissing`() {
        val entries = prepareEntriesFromFile("data/basic/02-arrayAtRoot-propertiesMissing.json", "/")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["name"]}).isNull()
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "2"))
        Assertions.assertThat(entries).element(1).extracting({it["name"]}).isEqualTo(CrunchProperty.String("name", "Ondra"))
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "3"))
        Assertions.assertThat(entries).element(2).extracting({it["born"]}).isEqualTo(CrunchProperty.String("born", "2016"))
    }

    @Test
    fun `testConvertJson_03-arrayAtRoot-varyingProperties`() {
        val entries = prepareEntriesFromFile("data/basic/03-arrayAtRoot-varyingProperties.json", "/")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["name"]}).isEqualTo(CrunchProperty.String("name", "Ada"))
        Assertions.assertThat(entries).element(0).extracting({it["surname"]}).isNull()
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "2"))
        Assertions.assertThat(entries).element(1).extracting({it["surname"]}).isEqualTo(CrunchProperty.String("surname", "Zizka"))
        Assertions.assertThat(entries).element(1).extracting({it["name"]}).isNull()
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "3"))
        Assertions.assertThat(entries).element(2).extracting({it["name"]}).isEqualTo(CrunchProperty.String("name", "Ondra"))
        Assertions.assertThat(entries).element(2).extracting({it["surname"]}).isNull()
    }

    @Test
    fun `testConvertJson_04-arrayAtRoot-differentTypes`() {
        val entries = prepareEntriesFromFile("data/basic/04-arrayAtRoot-differentTypes.json", "/")

        Assertions.assertThat(entries).size().isEqualTo(5)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["age"]}).isEqualTo(CrunchProperty.String("age", "4"))
        Assertions.assertThat(entries).element(1).extracting({it["age"]}).isEqualTo(CrunchProperty.Number("age", 15))
        Assertions.assertThat(entries).element(2).extracting({it["age"]}).isEqualTo(CrunchProperty.Boolean("age", true))
        Assertions.assertThat(entries).element(3).extracting({it["age"]}).isEqualTo(CrunchProperty.Null("age"))
        Assertions.assertThat(entries).element(4).extracting({it["age"]}).isNull()
    }

    @Test
    fun `testConvertJson_10-arrayAtRoot-nestedObject`() {
        val entries = prepareEntriesFromFile("data/basic/10-arrayAtRoot-nestedObject.json", "/")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "1"))
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "2"))
        // {"id": "3", "name": "Ondra", "address": {"city":  "Zurich", "zip": 8001}}
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "3"))
        Assertions.assertThat(entries).element(2).extracting({it["name"]}).isEqualTo(CrunchProperty.String("name", "Ondra"))
        Assertions.assertThat(entries).element(2).extracting({it["address"]}).isNull()
        Assertions.assertThat(entries).element(2).extracting({it["address.city"]}).isEqualTo(CrunchProperty.String("address.city", "Zurich"))
        Assertions.assertThat(entries).element(2).extracting({it["address.zip"]}).isEqualTo(CrunchProperty.Number("address.zip", 8001))
    }


    @Test
    fun `testConvertJson_11-arrayAtRoot-nestedArray`() {
        val entries = prepareEntriesFromFile("data/basic/11-arrayAtRoot-nestedArray.json", "/")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "1"))
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "2"))
        // {"id": "3", "name": "Ondra", "addresses": [{"city":  "Zurich", "zip": 8001}]}
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "3"))
        Assertions.assertThat(entries).element(2).extracting({it["name"]}).isEqualTo(CrunchProperty.String("name", "Ondra"))
        Assertions.assertThat(entries).element(2).extracting({it["addresses"]}).isEqualTo(CrunchProperty.Array("addresses", listOf())) // TODO - currently empty.
        //Assertions.assertThat(entries).element(2).extracting({it["addresses.1.city"]}).isEqualTo(MyProperty.String("addresses.1.city", "Zurich"))
        //Assertions.assertThat(entries).element(2).extracting({it["addresses.1.zip"]}).isEqualTo(MyProperty.Number("addresses.1.zip", 8001))
    }

    @Test
    fun `testConvertJson_12-arrayInObject-sameProperties`() {
        val entries = prepareEntriesFromFile("data/basic/12-arrayInObject-sameProperties.json", "/items")

        Assertions.assertThat(entries).size().isEqualTo(3)
        Assertions.assertThat(entries).element(0).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "1"))
        Assertions.assertThat(entries).element(0).extracting({it["name"]}).isEqualTo(CrunchProperty.String("name", "Ondra"))
        Assertions.assertThat(entries).element(1).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "2"))
        Assertions.assertThat(entries).element(2).extracting({it["id"]}).isEqualTo(CrunchProperty.String("id", "3"))
    }

    @Test
    fun `testConvertJson_21_redditAll`() {
        val entries = prepareEntriesFromFile("$testDataDir/json/redditAll.json", "/data/children")

        Assertions.assertThat(entries).size().isEqualTo(25)
        //Assertions.assertThat(entries).allSatisfy (reqements = Consumer { entry -> Assertions.assertThat(it["kind"]).equals("t3")} )
        Assertions.assertThat(entries).element(0).extracting({it["data.ups"]}).isEqualTo(CrunchProperty.Number("data.ups", 13819))
        Assertions.assertThat(entries).element(24).extracting({it["data.ups"]}).isEqualTo(CrunchProperty.Number("data.ups", 9947))
        Assertions.assertThat(entries).element(0).extracting({it["data.gildings.gid_1"]}).isEqualTo(CrunchProperty.Number("data.gildings.gid_1", 2))
        Assertions.assertThat(entries).element(0).extracting({it["data.preview.images"]}).isEqualTo(CrunchProperty.Array("data.preview.images", mutableListOf()))
        Assertions.assertThat(entries).element(0).extracting({it["data.link_flair_richtext"]}).isEqualTo(CrunchProperty.Array("data.link_flair_richtext", mutableListOf()))
        Assertions.assertThat(entries).element(0).extracting({it["data.created"]}).isEqualTo(CrunchProperty.Number("data.created", 1636109245.0))
    }

    companion object { private val log = logger() }
}
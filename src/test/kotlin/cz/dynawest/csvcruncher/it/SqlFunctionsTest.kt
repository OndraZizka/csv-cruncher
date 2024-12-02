package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import cz.dynawest.csvcruncher.CsvCruncherTestUtils.testDataDir
import cz.dynawest.csvcruncher.util.SqlFunctions.jsonLeaf
import cz.dynawest.csvcruncher.util.SqlFunctions.jsonLeaves_impl
import cz.dynawest.csvcruncher.util.SqlFunctions.jsonSubtree
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.test.assertEquals


class SqlFunctionsTest {

    @Test
    fun test_jsonSubtree_impl(testInfo: TestInfo) {
        assertEquals("\"bar\"", jsonSubtree("foo", """{ "foo": "bar" }"""))
        assertEquals("{\"bar\":\"baz\"}", jsonSubtree("foo", """{ "foo": { "bar": "baz" } }"""))
        assertEquals("{\"bar\":\"baz\"}", jsonSubtree("foo", """{ "foo": { "bar": "baz" } }"""))
        assertEquals("\"baz\"", jsonSubtree("foo/bar", """{ "foo": { "bar": "baz" } }"""))
        assertEquals("null", jsonSubtree("foo/bar", """{ "foo": { "bar": null } }"""))
        assertEquals(null, jsonSubtree("foo/NON-EXISTENT", """{ "foo": { "bar": "baz" } }"""))
        assertThrows<IllegalArgumentException> { jsonSubtree("foo/BAD-JSON", """{ ][{ dfwq3 /]-json": } """) }
    }

    @Test
    fun test_jsonLeaf_impl(testInfo: TestInfo) {
        assertEquals("bar", jsonLeaf("foo", """{ "foo": "bar" }"""))
        assertEquals(null, jsonLeaf("foo", """{ "foo": { "bar": "baz" } }""", true))
        assertThrows<IllegalArgumentException> { jsonLeaf("foo", """{ "foo": { "bar": "baz" } }""", false) }
        assertEquals("baz", jsonLeaf("foo/bar", """{ "foo": { "bar": "baz" } }"""))
        assertEquals("", jsonLeaf("foo/bar", """{ "foo": { "bar": "" } }"""))
        assertEquals("null", jsonLeaf("foo/bar", """{ "foo": { "bar": null } }"""))
        assertEquals(null, jsonLeaf("foo/NON-EXISTENT", """{ "foo": { "bar": "baz" } }"""))
        assertThrows<IllegalArgumentException> { jsonLeaf("foo/BAD-JSON", """{ ][{ dfwq3 /]-json": } """) }
    }

    @Test
    fun test_jsonLeaves_impl(testInfo: TestInfo) {
        assertEquals(listOf("bar"), jsonLeaves_impl("foo", """{ "foo": ["bar"] }"""))
        assertEquals(null, jsonLeaves_impl("foo", """{ "foo": { "bar": "baz" } }""", true))
        assertThrows<IllegalArgumentException> { jsonLeaves_impl("foo", """{ "foo": { "bar": "baz" } }""", false) }
        assertEquals(listOf("baz"), jsonLeaves_impl("foo/bar", """{ "foo": { "bar": ["baz"] } }"""))
        assertEquals(listOf(""), jsonLeaves_impl("foo/bar", """{ "foo": { "bar": [""] } }"""))
        assertEquals(listOf(null), jsonLeaves_impl("foo/bar", """{ "foo": { "bar": [null] } }"""))
        assertEquals(null, jsonLeaves_impl("foo/NON-EXISTENT", """{ "foo": { "bar": "baz" } }""", false))
        assertThrows<IllegalArgumentException> { jsonLeaves_impl("foo/BAD-JSON", """{ ][{ dfwq3 /]-json": } """) }
    }

    @Suppress("unused")
    enum class JsonTestCase (val expected: String?, val path: String, val json: String) {
        C1("\"bar\"",           "foo",      """{ "foo": "bar" }"""),
        C2("{\"bar\":\"baz\"}", "foo",      """{ "foo": { "bar": "baz" } }"""),
        C3("{\"bar\":\"baz\"}", "foo",      """{ "foo": { "bar": "baz" } }"""),
        C4("\"baz\"",           "foo/bar",  """{ "foo": { "bar": "baz" } }"""),
        C5(null,                "foo/NON-EXISTENT", """{ "foo": "bar" }"""),
        C6("null",                "foo/bar", """{ "foo": { "bar": null } }"""),
    }

    @ParameterizedTest
    @EnumSource(value = JsonTestCase::class)
    fun test_(case: JsonTestCase, testInfo: TestInfo) {
        val inPath = Paths.get("$testDataDir/eapBuilds.csv")
        val outputPath = """${testInfo.testMethod.get().name}.json"""
        val command = """ -in  | $inPath | -out | $outputPath | -sql | SELECT jsonSubtree('${case.path}', '${case.json}') FROM (VALUES(0)) AS dual"""

        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(Path.of(outputPath)).exists().isNotEmptyFile()
        // TBD assertions
    }


    @Test
    fun test_jsonSubtree_inSql(testInfo: TestInfo) {

        val inPath = Paths.get("$testDataDir/eapBuilds.csv")

        val outputPath = """${testInfo.testMethod.get().name}.json"""
        val command = """ -in  | $inPath | -out | $outputPath | -sql | SELECT jsonSubtree('foo', '{ "foo": "bar" }') FROM (VALUES(0)) AS dual"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(Path.of(outputPath)).exists().isNotEmptyFile()
        // TBD assertions

        // TBD: Bug - two outputs:
        //INFO:  * CSV output: /home/o/uw/csv-cruncher/test_jsonSubtree_inSql.json
        //INFO:  * JSON output: /home/o/uw/csv-cruncher/test_jsonSubtree_inSql.json
    }

    @Test
    fun test_jsonLeaf_inSql(testInfo: TestInfo) {

        val inPath = Paths.get("$testDataDir/eapBuilds.csv")

        val outputPath = """${testInfo.testMethod.get().name}.json"""
        val command = """ -in  | $inPath | -out | $outputPath | -sql | SELECT jsonLeaf('foo', '{ "foo": "bar" }', true) FROM (VALUES(0)) AS dual"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(Path.of(outputPath)).exists().isNotEmptyFile()
        // TBD assertions
    }

    @Test //@Disabled("Seems this is not supported by HSQLDB.")  Currently, returns a JSON string with the array. Not too useful.
    fun test_jsonLeaves_inSql(testInfo: TestInfo) {

        val inPath = Paths.get("$testDataDir/eapBuilds.csv")
        val outputPath = """${testInfo.testMethod.get().name}.json"""
        val command = """ -in  | $inPath | -out | $outputPath | -sql | SELECT jsonLeaves('foo', '{ "foo": ["bar"] }', true) FROM (VALUES(0)) AS dual"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        assertThat(Path.of(outputPath)).exists().isNotEmptyFile()
        // TBD assertions
    }

}
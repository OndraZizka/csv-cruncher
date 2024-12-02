package cz.dynawest.csvcruncher.it

import cz.dynawest.csvcruncher.CsvCruncherTestUtils
import cz.dynawest.csvcruncher.CsvCruncherTestUtils.testDataDir
import cz.dynawest.csvcruncher.util.SqlFunctions.jsonSubtree
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Paths
import kotlin.test.assertEquals

class SqlFunctionsTest {

    @Test
    fun test_jsonSubtree_impl(testInfo: TestInfo) {
        assertEquals("\"bar\"", jsonSubtree("foo", """{ "foo": "bar" }"""))
        assertEquals("{\"bar\":\"baz\"}", jsonSubtree("foo", """{ "foo": { "bar": "baz" } }"""))
        assertEquals("{\"bar\":\"baz\"}", jsonSubtree("foo", """{ "foo": { "bar": "baz" } }"""))
        assertEquals("\"baz\"", jsonSubtree("foo/bar", """{ "foo": { "bar": "baz" } }"""))
        assertEquals(null, jsonSubtree("foo/NON-EXISTENT", """{ "foo": { "bar": "baz" } }"""))
        assertThrows<IllegalArgumentException> { jsonSubtree("foo/BAD-JSON", """{ ][{ dfwq3 /]-json": } """) }
    }

    @Suppress("unused")
    enum class JsonTestCase (val expected: String?, val path: String, val json: String) {
        C1("\"bar\"",           "foo",      """{ "foo": "bar" }"""),
        C2("{\"bar\":\"baz\"}", "foo",      """{ "foo": { "bar": "baz" } }"""),
        C3("{\"bar\":\"baz\"}", "foo",      """{ "foo": { "bar": "baz" } }"""),
        C4("\"baz\"",           "foo/bar",  """{ "foo": { "bar": "baz" } }"""),
        C5(null,                "foo/NON-EXISTENT", """{ "foo": { "bar": "baz" } }"""),
    }

    @ParameterizedTest
    @EnumSource(value = JsonTestCase::class)
    fun test_(case: JsonTestCase) {
        val inPath = Paths.get("$testDataDir/eapBuilds.csv")
        val command = """ -in  | $inPath | -out | - | -sql | SELECT jsonSubtree('${case.path}', '${case.json}') FROM (VALUES(0)) AS dual"""

        // Capture the stdout
        val resultBytes = ByteArrayOutputStream()
        val resultStream = PrintStream(resultBytes)
        //System.setOut(resultStream) // TBD - put to files after all. This is too complicated.

        CsvCruncherTestUtils.runCruncherWithArguments(command)

        resultBytes.close()
    }


    @Test
    fun test_jsonSubtree_inSql(testInfo: TestInfo) {

        val inPath = Paths.get("$testDataDir/eapBuilds.csv")
        val command = """ -in  | $inPath | -out | - | -sql | SELECT jsonSubtree('foo', '{ "foo": "bar" }') FROM (VALUES(0)) AS dual"""
        CsvCruncherTestUtils.runCruncherWithArguments(command)

        //assertThat(outputPath).exists().isNotEmptyFile() // STDOUT for now
    }

}
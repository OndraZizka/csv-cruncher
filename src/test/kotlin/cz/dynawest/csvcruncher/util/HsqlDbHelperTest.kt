package cz.dynawest.csvcruncher.util

import cz.dynawest.csvcruncher.HsqlDbHelper
import cz.dynawest.csvcruncher.HsqlDbHelper.Companion.quoteIdentifiersInQuery
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo

class HsqlDbHelperTest {

    @Test
    fun test_quoteColumnNames_1(testInfo: TestInfo) {
        val sql = "SELECT a.bc.d FROM table1 WHERE a.bc.d=1 OR (a.bc.d>2 AND a.bc.d<4)"
        val sqlReplaced = quoteIdentifiersInQuery(sql, listOf("a.bc.d"))

        assertThat(sqlReplaced).isEqualTo("""SELECT "a.bc.d" FROM table1 WHERE "a.bc.d"=1 OR ("a.bc.d">2 AND "a.bc.d"<4)""")
    }

    @Test
    fun test_quoteColumnNames_2(testInfo: TestInfo) {
        val sql = "SELECT a.bc.d, bc+a.bc.d FROM table1 WHERE a.bc.d=1 OR (a.bc.d>2 AND a.bc.d<4)"
        val sqlReplaced = quoteIdentifiersInQuery(sql, listOf("a.bc.d", "bc"))

        assertThat(sqlReplaced).isEqualTo("""SELECT "a.bc.d", "bc"+"a.bc.d" FROM table1 WHERE "a.bc.d"=1 OR ("a.bc.d">2 AND "a.bc.d"<4)""")
    }

    @Test
    fun test_quoteColumnNames_aaaa(testInfo: TestInfo) {
        val sql = "SELECT a.a, a+\"a\", a+a, aa+aa, aa, aaa, aa.aa, (a<aa), a, a_a-a_a, \"a.a\" FROM table1"
        val sqlReplaced = quoteIdentifiersInQuery(sql, "a.a,a+a,aa,aaa,aa.aa,a,a_a".split(","))

        assertThat(sqlReplaced).isEqualTo("""SELECT "a.a", "a"+"a", "a+a", "aa"+"aa", "aa", "aaa", "aa.aa", ("a"<"aa"), "a", "a_a"-"a_a", "a.a" FROM table1""")
    }

    @Test fun test_setOrReplaceLimit(testInfo: TestInfo) {
        val expected = "SELECT 1 FROM x LIMIT 1"
        assertThat(HsqlDbHelper.setOrReplaceLimit("SELECT 1 FROM x LIMIT 20", "LIMIT 1")).isEqualTo(expected)
        assertThat(HsqlDbHelper.setOrReplaceLimit("SELECT 1 FROM x LIMIT 20 ", "LIMIT 1")).isEqualTo(expected)
        assertThat(HsqlDbHelper.setOrReplaceLimit("SELECT 1 FROM x  LIMIT  20  OFFSET  40 ", "LIMIT 1")).isEqualTo(expected)
        assertThat(HsqlDbHelper.setOrReplaceLimit("SELECT 1 FROM x LIMIT 1 ", "LIMIT 1")).isEqualTo(expected)
    }
}
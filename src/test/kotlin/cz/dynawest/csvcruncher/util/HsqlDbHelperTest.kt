package cz.dynawest.csvcruncher.util

import cz.dynawest.csvcruncher.HsqlDbHelper.Companion.quoteColumnNamesInQuery
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class HsqlDbHelperTest {

    @Test
    fun test_quoteColumnNames_1() {
        val sql = "SELECT a.bc.d FROM table1 WHERE a.bc.d=1 OR (a.bc.d>2 AND a.bc.d<4)"
        val sqlReplaced = quoteColumnNamesInQuery(sql, listOf("a.bc.d"))

        assertThat(sqlReplaced).isEqualTo("""SELECT "a.bc.d" FROM table1 WHERE "a.bc.d"=1 OR ("a.bc.d">2 AND "a.bc.d"<4)""")
    }

    @Test
    fun test_quoteColumnNames_2() {
        val sql = "SELECT a.bc.d, bc+a.bc.d FROM table1 WHERE a.bc.d=1 OR (a.bc.d>2 AND a.bc.d<4)"
        val sqlReplaced = quoteColumnNamesInQuery(sql, listOf("a.bc.d", "bc"))

        assertThat(sqlReplaced).isEqualTo("""SELECT "a.bc.d", "bc"+"a.bc.d" FROM table1 WHERE "a.bc.d"=1 OR ("a.bc.d">2 AND "a.bc.d"<4)""")
    }

    @Test
    fun test_quoteColumnNames_aaaa() {
        val sql = "SELECT a.a, a+a, aa, aaa, aa.aa, (a<aa), a, a_a-a_a, \"a.a\" FROM table1"
        val sqlReplaced = quoteColumnNamesInQuery(sql, "a.a,a+a,aa,aaa,aa.aa,a,a_a".split(","))

        assertThat(sqlReplaced).isEqualTo("""SELECT "a.a", "a"+"a", "aa", "aaa", "aa.aa", ("a"<"aa"), "a", "a_a"-"a_a", "a.a" FROM table1""")
    }

}
package cz.dynawest.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInfo


class ClassUtilsTest {

    @Test
    fun test_getCurrentClass(testInfo: TestInfo) {
        assertEquals(ClassUtilsTest::class.java.name, ClassUtils.geCurrentClassName())
    }

    @Test fun test_getCallingClass(testInfo: TestInfo) {
        assertEquals(ClassUtilsTest::class.java.name, TestWrapClass().caller())
    }

    private class TestWrapClass {
        fun caller() = ClassUtils.getCallingClassName()
    }
}


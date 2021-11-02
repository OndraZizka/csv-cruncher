package cz.dynawest.util

import junit.framework.Assert.assertEquals
import org.junit.Test

class ClassUtilsTest {

    @Test fun test_getCurrentClass() {
        assertEquals(ClassUtilsTest::class.java.name, ClassUtils.geCurrentClassName())
    }

    @Test fun test_getCallingClass() {
        assertEquals(ClassUtilsTest::class.java.name, TestWrapClass().caller())
    }

    private class TestWrapClass {
        fun caller() = ClassUtils.getCallingClassName()
    }
}


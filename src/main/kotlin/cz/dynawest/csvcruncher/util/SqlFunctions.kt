package cz.dynawest.csvcruncher.util

import com.fasterxml.jackson.databind.ObjectMapper
import cz.dynawest.csvcruncher.HsqlDbHelper
import java.nio.file.InvalidPathException
import java.nio.file.Path


@Suppress("unused")
object SqlFunctions {

    @JvmStatic
    fun defineSqlFunctions(hsqlDbHelper: HsqlDbHelper) {

        val FUNCTION_startsWith = "startsWith"
        hsqlDbHelper.executeSql("DROP FUNCTION IF EXISTS $FUNCTION_startsWith", "Error dropping Java function $FUNCTION_startsWith().")
        hsqlDbHelper.executeSql(
            """CREATE FUNCTION $FUNCTION_startsWith(whole LONGVARCHAR, startx LONGVARCHAR) 
                    RETURNS BOOLEAN
                    RETURNS NULL ON NULL INPUT
                    LANGUAGE JAVA PARAMETER STYLE JAVA
                    DETERMINISTIC
                    NO SQL
                    EXTERNAL NAME 'CLASSPATH:${javaClass.name}.startsWith'
                """,
            "Error creating Java function $FUNCTION_startsWith()."
        )

        // TODO: Add a test.
        val FUNCTION_jsonSubtree = "jsonSubtree"
        hsqlDbHelper.executeSql("DROP FUNCTION IF EXISTS $FUNCTION_jsonSubtree", "Error dropping Java function $FUNCTION_jsonSubtree().")
        hsqlDbHelper.executeSql(
            """CREATE FUNCTION $FUNCTION_jsonSubtree(path LONGVARCHAR, jsonString LONGVARCHAR) 
                    RETURNS LONGVARCHAR
                    RETURNS NULL ON NULL INPUT
                    DETERMINISTIC
                    NO SQL
                    LANGUAGE JAVA PARAMETER STYLE JAVA
                    EXTERNAL NAME 'CLASSPATH:${javaClass.name}.jsonSubtree'
                """,
            "Error creating Java function $FUNCTION_jsonSubtree()."
        )
    }

    @JvmStatic
    fun startsWith(whole: String, startx: String): Boolean {
        return whole.startsWith(startx)
    }

    @JvmStatic
    fun jsonSubtree(pathString: String, jsonString: String): String? {
        var path = try { Path.of(pathString) } catch (e: InvalidPathException) { throw IllegalArgumentException("Invalid path, use '/' to navigate (no array support): $pathString") }

        var tree =
            try { objectMapper.readTree(jsonString) }
            catch (e: Exception) { throw IllegalArgumentException("Failed parsing the JSON (truncated to 100): ${jsonString.take(100)}") }

        // Follow the path and return the respective subtree, as JSON.
        do {
            val nextStep = path.firstOrNull() ?: break
            tree = tree.get( nextStep.toString() )
            if (tree == null) return null

            if (path.nameCount <= 1) break; // Otherwise the next step would fail - Path can't get empty.

            ///System.out.println("PATH ${path.nameCount}: $path")
            path = path.subpath(1, path.nameCount)
        }
        while (true)

        return objectMapper.writeValueAsString(tree)
    }

    private val objectMapper: ObjectMapper by lazy { ObjectMapper() }

}
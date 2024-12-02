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
    fun jsonSubtree(path: String, jsonString: String): String? {
        var path_ = try { Path.of(path) } catch (e: InvalidPathException) { throw IllegalArgumentException("Invalid path, use '/' to navigate (no array support): $path") }

        val objectMapper = ObjectMapper()

        var tree = try {
            objectMapper.readTree(jsonString)
        }
        catch (e: Exception) { throw IllegalArgumentException("Failed parsing the JSON: ${jsonString.take(20)}") }

        // TODO: Parse JSON and return a subtree.
        while (path_.firstOrNull() != null) {
            tree = tree.get( path_.firstOrNull()!!.toString() )
            if (tree == null) return null

            path_ = path_.subpath(1, path.length)
        }

        return objectMapper.writeValueAsString(tree)
    }
}

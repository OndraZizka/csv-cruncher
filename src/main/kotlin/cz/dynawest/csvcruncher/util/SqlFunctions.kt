package cz.dynawest.csvcruncher.util

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
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


        val FUNCTION_jsonLeaf = "jsonLeaf"
        hsqlDbHelper.executeSql("DROP FUNCTION IF EXISTS $FUNCTION_jsonLeaf", "Error dropping Java function $FUNCTION_jsonLeaf().")
        hsqlDbHelper.executeSql(
            """CREATE FUNCTION $FUNCTION_jsonLeaf(path LONGVARCHAR, jsonString LONGVARCHAR, nullOnNonScalarResult BOOLEAN) 
                    RETURNS LONGVARCHAR
                    RETURNS NULL ON NULL INPUT
                    DETERMINISTIC
                    NO SQL
                    LANGUAGE JAVA PARAMETER STYLE JAVA
                    EXTERNAL NAME 'CLASSPATH:${javaClass.name}.jsonLeaf'
                """,
            "Error creating Java function $FUNCTION_jsonLeaf()."
        )


        /* Can't make this work.
        hsqlDbHelper.executeSql("CREATE TYPE ARRAY_STRINGS AS VARCHAR ARRAY", "Error creating type ARRAY_STRINGS.")
        hsqlDbHelper.executeSql("SET DATABASE EVENT LOG SQL LEVEL 3", "Error creating type ARRAY_STRINGS.")

        val FUNCTION_simpleArray = "simpleArray"
        hsqlDbHelper.executeSql("DROP FUNCTION IF EXISTS $FUNCTION_simpleArray", "Error dropping Java function $FUNCTION_simpleArray().")
        hsqlDbHelper.executeSql(
                """CREATE FUNCTION simpleArray()
                        RETURNS VARCHAR ARRAY
                        LANGUAGE JAVA PARAMETER STYLE JAVA
                        EXTERNAL NAME 'CLASSPATH:${javaClass.name}.simpleArray'
                   """,
            "Error creating Java function $FUNCTION_simpleArray()."
        )*/


        val FUNCTION_jsonLeaves = "jsonLeaves"
        hsqlDbHelper.executeSql("DROP FUNCTION IF EXISTS $FUNCTION_jsonLeaves", "Error dropping Java function $FUNCTION_jsonLeaves().")
        hsqlDbHelper.executeSql(
            """CREATE FUNCTION $FUNCTION_jsonLeaves(pathToArray LONGVARCHAR, leavesSubPath LONGVARCHAR, jsonString LONGVARCHAR, nullOnNonArrayNode BOOLEAN) 
                    RETURNS LONGVARCHAR -- ARRAY -- ARRAY_STRINGS -- ARRAY doesn't work for functions :/
                    RETURNS NULL ON NULL INPUT
                    DETERMINISTIC
                    NO SQL
                    LANGUAGE JAVA PARAMETER STYLE JAVA
                    EXTERNAL NAME 'CLASSPATH:${javaClass.name}.jsonLeaves'
                """,
            "Error creating Java function $FUNCTION_jsonLeaves()."
        )

    }

    @JvmStatic
    fun startsWith(whole: String, startx: String): Boolean {
        return whole.startsWith(startx)
    }

    private fun findJsonSubtree(pathString: String, jsonString: String): JsonNode? {
        var path = try { Path.of(pathString) } catch (e: InvalidPathException) { throw IllegalArgumentException("Invalid path, use '/' to navigate (no array support): $pathString") }

        var tree =
            try { objectMapper.readTree(jsonString) }
            catch (e: Exception) { throw IllegalArgumentException("Failed parsing the JSON (truncated to 100): ${jsonString.take(100)}") }

        // Follow the path and return the respective subtree, as JSON.
        do {
            val nextStep = path.firstOrNull() ?: break
            tree = tree.get( nextStep.toString() )
            if (tree == null) return null

            if (path.nameCount <= 1) break // Otherwise the next step would fail - Path can't get empty.

            ///System.out.println("PATH ${path.nameCount}: $path")
            path = path.subpath(1, path.nameCount)
        }
        while (true)

        return tree
    }

    /** Returns the respective subtree as a JSON string. */
    @JvmStatic
    fun jsonSubtree(pathString: String, jsonString: String): String? {
        val tree = findJsonSubtree(pathString, jsonString)
        //if (tree?.isNull != false) return null
        if (tree == null) return null

        return objectMapper.writeValueAsString(tree)
    }

    /** Returns the raw value of the respective leaf, converted to a text. */
    @JvmStatic
    fun jsonLeaf(pathString: String, jsonString: String, nullOnNonScalarResult: Boolean = false): String? {
        val tree = findJsonSubtree(pathString, jsonString) ?: return null

        if (tree.isValueNode) return tree.asText()

        if (nullOnNonScalarResult) return null

        throw IllegalArgumentException("The node at $pathString is not a scalar value, but ${tree.nodeType.name}.")
    }

    /** Returns the raw value of the respective leaf, converted to a text. */
    @JvmStatic
    fun jsonLeaves(pathToArray: String, jsonPointer: String, jsonString: String, nullOnNonArrayNode: Boolean = false): String? {
        return jsonLeaves_impl(pathToArray, jsonPointer, jsonString, nullOnNonArrayNode)
            ?.let { objectMapper.writeValueAsString(it) }
    }

    /*
    @JvmStatic
    fun jsonLeaves_array(pathToArray: String, jsonString: String, subPath: String, nullOnNonArrayNode: Boolean = false): Array<String?>? {
        return jsonLeaves_impl(pathToArray, jsonString, subPath, nullOnNonArrayNode)?.toTypedArray()
        //return jsonLeaves_impl(pathToArray, jsonString, nullOnNonArrayNode)?.toTypedArray()?.requireNoNulls() ?: emptyArray<String>()
    }
     */

    /**
     * For JsonPointer see https://datatracker.ietf.org/doc/html/draft-ietf-appsawg-json-pointer-03 .
     */
    internal fun jsonLeaves_impl(pathToArray: String, leavesSubPath: String, jsonString: String, nullOnNonArrayNode: Boolean = false): List<String?>? {
        val tree = findJsonSubtree(pathToArray, jsonString) ?: return null

        if (!tree.isArray) {
            if (nullOnNonArrayNode) return null
            throw IllegalArgumentException("The node at $pathToArray is not an array value, but ${tree.nodeType.name}.")
        }

        //@Suppress("NAME_SHADOWING")
        //val leavesSubPath = "/" + leavesSubPath.removePrefix("/")

        return (tree as ArrayNode).map {
            //objectMapper.writeValueAsString(it)
            it.at(leavesSubPath).textValue()
        }
    }


    @JvmStatic
    fun simpleArray(): Array<String> = arrayOf("a", "b", "c")

    private val objectMapper: ObjectMapper by lazy { ObjectMapper() }

}
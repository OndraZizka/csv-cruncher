package cz.dynawest.csvcruncher.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.InputStream
import java.net.URL
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.jar.Attributes
import java.util.jar.JarFile
import java.util.jar.Manifest

/**
 * Creates a logger for the containing class.
 * Usage:
 *      companion object {
 *          private val log = logger()
 *      }
 */
fun Any.logger(): Logger {
    val clazz = if (this::class.isCompanion) this::class.java.enclosingClass else this::class.java
    return LoggerFactory.getLogger(clazz)
}


/*
 * Note that this source code was auto-migrated so it's a bit ugly.
 */
object Utils {

    @JvmStatic
    fun resolvePathToUserDirIfRelative(path: Path): File {
        return if (path.isAbsolute) path.toFile() else Paths.get(System.getProperty("user.dir")).resolve(path).toFile()
    }

    @JvmStatic
    fun escapeSql(str: String): String {
        return str.replace("'", "''")
    }

    /**
     * Returns a map with keys from the given list, and null values. Doesn't deal with duplicate keys.
     */
    fun listToMapKeysWithNullValues(keys: List<String>): Map<String, String?> {
        val result = LinkedHashMap<String, String?>()
        for (columnsName in keys) {
            result[columnsName] = null
        }
        return result
    }
}
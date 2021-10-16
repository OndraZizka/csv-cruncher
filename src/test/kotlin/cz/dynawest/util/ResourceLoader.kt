package cz.dynawest.util

import java.io.FileNotFoundException
import java.io.InputStream
import java.nio.file.Path

object ResourceLoader {

    fun getResourceFullPathFromRelative(resourcePathFromCallingClass: String, fqcn: String): Path {
        val packageClassPath = Path.of(fqcn.replace('.', '/')).parent
        return packageClassPath.resolve(Path.of(resourcePathFromCallingClass).normalize())
    }

    fun openResourceAtRelativePath(resourcePath: Path): InputStream {
        val path = getResourceFullPathFromRelative(resourcePath.toString(), ClassUtils.getCallingClassName() ?: throw IllegalStateException("Can't find calling class."))
        return javaClass.classLoader!!.getResourceAsStream(path.toString())
            ?: throw FileNotFoundException("Resource '$path' not found in the classpath.")
    }

    fun openResourceAtRelativePath(resourcePath: String): InputStream = openResourceAtRelativePath(Path.of(resourcePath))

    fun loadResourceAtRelativePath(resourcePath: Path): String {
        val inputStream = openResourceAtRelativePath(resourcePath)
        return inputStream.use { it.reader().readText() }
    }

    fun loadResourceAtRelativePath(resourcePath: String) = loadResourceAtRelativePath(Path.of(resourcePath))

}
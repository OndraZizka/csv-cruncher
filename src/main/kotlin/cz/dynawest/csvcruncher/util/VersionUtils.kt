package cz.dynawest.csvcruncher.util

import java.io.InputStream
import java.net.URL
import java.util.*
import java.util.jar.Attributes
import java.util.jar.JarFile
import java.util.jar.Manifest

object VersionUtils {
    val versionFilePath = JarFile.MANIFEST_NAME;

    @JvmStatic
    val version: String?
        get() = readVersionFromMavenPom("ch.zizka.csvcruncher", "csv-cruncher")


    /** Only works when running from a .jar . */
    private fun readVersionFromMavenPom(groupId: String, artifactid: String): String? {
        val versionFilePath = "META-INF/maven/$groupId/$artifactid/pom.properties"

        try {
            val inStream = Utils::class.java.classLoader.getResourceAsStream(versionFilePath)
            val props = Properties()
            props.load(inStream)
            return props.getProperty("version")
        }
        catch (ex: Exception) {
            log.warn("Invalid " + versionFilePath + ": " + ex.message)
        }
        return null
    }

    private fun readVersionFromManifest(): String? {
        //val versionKey = "Implementation-Version";
        //val versionKey = "Release-Version";

        try {
            val resourcesEnumeration = Thread.currentThread().getContextClassLoader().getResources(versionFilePath)

            while (resourcesEnumeration.hasMoreElements()) {
                val url: URL = resourcesEnumeration.nextElement()
                url.openStream().use { inStream ->
                    if (inStream == null) {
                        log.warn("Can't read resource at " + url.toString())
                        return null
                    }
                    if (true) {
                        val manifest = Manifest(inStream)
                        val mainAttribs: Attributes = manifest.getMainAttributes()
                        val version: String = mainAttribs.getValue("version")
                        return version
                    }
                    else {
                        val props = Properties()
                        props.load(inStream)
                        return props.getProperty("version")
                    }
                }
            }
        }
        catch (ex: Exception) {
            log.warn("Invalid " + versionFilePath + ": " + ex.message)
        }
        return null
    }

    private val log = logger()
}

fun main() {
    println(VersionUtils.version)
}
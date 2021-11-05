package cz.dynawest.csvcruncher.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

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


    /*resEnum = Thread.currentThread().getContextClassLoader().getResources(versionFilePath);
    while (resEnum.hasMoreElements()) {
        URL url = (URL)resEnum.nextElement();
        //LOG.info("AAA" + url);
        InputStream is = url.openStream();
        if (is == null) {
            LOG.warning("Can't read resource at " + url.toString());
            return null;
        }
        if (isManifest) {
            Manifest manifest = new Manifest(is);
            Attributes mainAttribs = manifest.getMainAttributes();
            String version = mainAttribs.getValue(versionKey);
            if (version != null) {
                return version;
            }
        }
        else {
            Properties props = new Properties();
            props.load(is);
            String version = props.getProperty(versionKey);
        }
    }*/


    //String versionFilePath = JarFile.MANIFEST_NAME;
    //String versionKey = "Implementation-Version";
    //String versionKey = "Release-Version";
    @JvmStatic
    val version: String?
        get() {
            //String versionFilePath = JarFile.MANIFEST_NAME;
            //String versionKey = "Implementation-Version";
            //String versionKey = "Release-Version";
            val versionFilePath = "META-INF/maven/cz.dynawest.csvcruncher/CsvCruncher/pom.properties"
            val versionKey = "version"

            try {
                val inStream = Utils::class.java.classLoader.getResourceAsStream(versionFilePath)
                val props = Properties()
                props.load(inStream)
                //props.list(System.out);
                return props.getProperty(versionKey)

                /*resEnum = Thread.currentThread().getContextClassLoader().getResources(versionFilePath);
                while (resEnum.hasMoreElements()) {
                    URL url = (URL)resEnum.nextElement();
                    //LOG.info("AAA" + url);
                    InputStream is = url.openStream();
                    if (is == null) {
                        LOG.warning("Can't read resource at " + url.toString());
                        return null;
                    }
                    if (isManifest) {
                        Manifest manifest = new Manifest(is);
                        Attributes mainAttribs = manifest.getMainAttributes();
                        String version = mainAttribs.getValue(versionKey);
                        if (version != null) {
                            return version;
                        }
                    }
                    else {
                        Properties props = new Properties();
                        props.load(is);
                        String version = props.getProperty(versionKey);
                    }
                }*/
            }
            catch (ex: Exception) {
                log.warn("Invalid " + versionFilePath + ": " + ex.message)
            }
            return null
        }

    private val log = logger()
}
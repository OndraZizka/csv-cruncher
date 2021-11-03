package cz.dynawest.csvcruncher.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
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


object Utils {

    private val log = logger()

    @JvmStatic
    fun resolvePathToUserDirIfRelative(path: Path): File {
        return if (path.isAbsolute) path.toFile() else Paths.get(System.getProperty("user.dir")).resolve(path).toFile()
    }

    @JvmStatic
    fun escapeSql(str: String): String {
        return str.replace("'", "''")
    }//props.list(System.out);

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
            val isManifest = false
            var resEnum: Enumeration<*>
            try {
                val `is` = Utils::class.java.classLoader.getResourceAsStream(versionFilePath)
                val props = Properties()
                props.load(`is`)
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
            } catch (ex: Exception) {
                log.warn("Invalid " + versionFilePath + ": " + ex.message)
            }
            return null
        }
    /* This would need a bit of reflection or using normal class rather than enum to represent an option.
    public static <EnumClass extends Enum> EnumClass processOptionIfMatches(String arg, Class<EnumClass> enumClass)
    {
        if (
                !arg.equals("--" + enumClass. ...?) &&
                !arg.startsWith("--" + enumClass. ...? + "=")
        )

        for each enum option
            if (arg.endsWith("=" + enumOption.getOptionValue()))
            return
        else
            throw new IllegalArgumentException(String.format(
                    "Unknown value for %s: %s Try one of %s",
                    Options.CombineInputFiles.PARAM_NAME, arg,
                    EnumUtils.getEnumList(Options.CombineInputFiles.class).stream().map(Options.CombineInputFiles::getOptionValue)));

    }
    / **/
    /**
     * This is for the case we use hand-made JSON marshalling.
     * Returns null if the column value was null, or if the returned type is not supported.
     */
    @Throws(SQLException::class)
    private fun formatValueForJson(resultSet: ResultSet, colIndex: Int, colsAreNumbers: BooleanArray): String? {
        val metaData = resultSet.metaData
        val `val`: String
        `val` = when (metaData.getColumnType(colIndex)) {
            Types.VARCHAR, Types.CHAR, Types.CLOB -> resultSet.getString(colIndex)
            Types.TINYINT, Types.BIT -> "" + resultSet.getByte(colIndex)
            Types.SMALLINT -> "" + resultSet.getShort(colIndex)
            Types.INTEGER -> "" + resultSet.getInt(colIndex)
            Types.BIGINT -> "" + resultSet.getLong(colIndex)
            Types.BOOLEAN -> "" + resultSet.getBoolean(colIndex)
            Types.FLOAT -> "" + resultSet.getFloat(colIndex)
            Types.DOUBLE, Types.DECIMAL -> "" + resultSet.getDouble(colIndex)
            Types.NUMERIC -> "" + resultSet.getBigDecimal(colIndex)
            Types.DATE -> "" + resultSet.getDate(colIndex)
            Types.TIME -> "" + resultSet.getTime(colIndex)
            Types.TIMESTAMP -> ("" + resultSet.getTimestamp(colIndex)).replace(' ', 'T')
            else -> {
                log.error("Unsupported type of column " + metaData.getColumnLabel(colIndex) + ": " + metaData.getColumnTypeName(colIndex))
                return null
            }
        }
        return if (resultSet.wasNull()) null else `val`
    }
}
package cz.dynawest.csvcruncher;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.logging.Logger;

public class Utils
{
    private static final Logger log = Logger.getLogger(Utils.class.getName());

    static File resolvePathToUserDirIfRelative(Path path)
    {
        return path.isAbsolute() ? path.toFile() : Paths.get(System.getProperty("user.dir")).resolve(path).toFile();
    }

    static String escapeSql(String str)
    {
        return str.replace("'", "''");
    }

    public static String getVersion()
    {
        //String versionFilePath = JarFile.MANIFEST_NAME;
        //String versionKey = "Implementation-Version";
        //String versionKey = "Release-Version";

        String versionFilePath = "META-INF/maven/cz.dynawest.csvcruncher/CsvCruncher/pom.properties";
        String versionKey = "version";


        boolean isManifest = false;

        Enumeration resEnum;
        try {
            InputStream is = Utils.class.getClassLoader().getResourceAsStream(versionFilePath);
            Properties props = new Properties();
            props.load(is);
            String version = props.getProperty(versionKey);
            //props.list(System.out);
            return version;

            /*resEnum = Thread.currentThread().getContextClassLoader().getResources(versionFilePath);
            while (resEnum.hasMoreElements()) {
                URL url = (URL)resEnum.nextElement();
                //log.info("AAA" + url);
                InputStream is = url.openStream();
                if (is == null) {
                    log.warning("Can't read resource at " + url.toString());
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
        } catch (Exception ex) {
            log.warning("Invalid " + versionFilePath + ": " + ex.getMessage());
        }
        return null;
    }
}

package cz.dynawest.logging;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class LoggingUtils
{
    private static final Logger log = Logger.getLogger(LoggingUtils.class.getName());
    private static final String DEFAULT_CLASSPATH_SEARCH_PATH = "logging.properties";
    private static final String DW_DEFAULT_PROPS_PATH = "/cz/dynawest/logging/logging-default.properties";

    public static void initLogging()
    {
        initLogging("#/" + DEFAULT_CLASSPATH_SEARCH_PATH);
    }

    public static void initLogging(String filePath)
    {
        boolean wasFromSysProp = true;
        String logConfigFile = System.getProperty("java.util.logging.config.file");
        if (logConfigFile == null) {
            logConfigFile = filePath;
            wasFromSysProp = false;
        }

        try {
            InputStream configIS;
            if (logConfigFile.startsWith("#/")) {
                configIS = LoggingUtils.class.getResourceAsStream(logConfigFile.substring(1));
            }
            else {
                try {
                    configIS = new FileInputStream(logConfigFile);
                }
                catch (IOException ex) {
                    configIS = null;
                }
            }

            log.fine("Loading logging conf from: " + logConfigFile + (!wasFromSysProp ? "" : " (set in sys var java.util.logging.config.file)"));
            if (configIS == null) {
                log.info("Log config file not found: " + logConfigFile + "  Using LoggingUtils' default.");
                logConfigFile = DW_DEFAULT_PROPS_PATH;
                configIS = LoggingUtils.class.getResourceAsStream(logConfigFile);
            }

            LogManager.getLogManager().readConfiguration((InputStream) configIS);
        }
        catch (IOException var6) {
            System.err.println("Error loading logging conf from [" + logConfigFile + "]. Using JDK\'s default.");
        }

    }
}

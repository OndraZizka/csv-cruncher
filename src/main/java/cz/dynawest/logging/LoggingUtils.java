package cz.dynawest.logging;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class LoggingUtils
{
    private static final Logger log = Logger.getLogger(LoggingUtils.class.getName());
    private static final String DEFAULT_CLASSPATH_SEARCH_PATH = "/logging.properties";
    private static final String DW_DEFAULT_PROPS_PATH = "/cz/dynawest/logging/logging-default.properties";

    public static void initLogging()
    {
        initLogging("#/logging.properties");
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
            Object ex;
            if (logConfigFile.startsWith("#")) {
                ex = LoggingUtils.class.getResourceAsStream(logConfigFile.substring(1));
            }
            else {
                try {
                    ex = new FileInputStream(logConfigFile);
                }
                catch (IOException var5) {
                    ex = null;
                }
            }

            log.info("Loading logging conf from: " + logConfigFile + (!wasFromSysProp ? "" : " (set in sys var java.util.logging.config.file)"));
            if (ex == null) {
                log.warning("Log config file not found: " + logConfigFile + "  Using LoggingUtils\' default.");
                logConfigFile = "/cz/dynawest/logging/logging-default.properties";
                ex = LoggingUtils.class.getResourceAsStream("/cz/dynawest/logging/logging-default.properties");
            }

            LogManager.getLogManager().readConfiguration((InputStream) ex);
        }
        catch (IOException var6) {
            System.err.println("Error loading logging conf from [" + logConfigFile + "]. Using JDK\'s default.");
        }

    }
}

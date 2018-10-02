package cz.dynawest.logging;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class LoggingUtils
{
    private static final Logger log = Logger.getLogger(LoggingUtils.class.getName());
    private static final String DEFAULT_CLASSPATH_SEARCH_PATH = "/logging.properties";
    private static final String DW_DEFAULT_PROPS_PATH = "/cz/dynawest/logging/logging-default.properties";

    public static void initLogging()
    {
        initLogging("#" + DEFAULT_CLASSPATH_SEARCH_PATH);
    }

    public static void initLogging(String filePath)
    {
        //dumpJulLoggersConfig();

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

            /*
            log.info("Logging test BEFORE");
            dumpJulLoggersConfig();
            forceFormatterToJulLoggers();
            dumpJulLoggersConfig();
            log.info("Logging test AFTER");
            /**/
        }
        catch (IOException ex) {
            System.err.println("Error loading logging conf from [" + logConfigFile + "]. Using JDK\'s default.");
        }

    }

    private static void dumpJulLoggersConfig()
    {
        System.out.println(" --- Loggers: ---");
        Collections.list(LogManager.getLogManager().getLoggerNames()).forEach(
            loggerName -> {
                List<Handler> handlers = Arrays.asList(Logger.getLogger(loggerName).getHandlers());
                System.out.println(" * Logger " + loggerName + ": " + handlers.size());
                handlers = Arrays.asList(Logger.getLogger(loggerName).getHandlers());
                handlers.forEach(handler -> System.out.println("     Handler Formatter: " + handler.getFormatter()));
            }
        );
    }
    private static void forceFormatterToJulLoggers()
    {
        Collections.list(LogManager.getLogManager().getLoggerNames()).forEach(
            loggerName -> {
                Logger logger = LogManager.getLogManager().getLogger(loggerName);
                logger.setUseParentHandlers(false);
                List<Handler> handlers = Arrays.asList(logger.getHandlers());
                if (handlers.size() == 0) {
                    logger.addHandler(new ConsoleHandler(){{this.setFormatter(new SingleLineFormatter());}});
                }
            }
        );
    }
}

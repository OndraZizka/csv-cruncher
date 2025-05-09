package cz.dynawest.csvcruncher.app

import ch.qos.logback.classic.Level
import cz.dynawest.csvcruncher.Cruncher
import cz.dynawest.csvcruncher.CsvCruncherException
import cz.dynawest.csvcruncher.SqlSyntaxCruncherException
import cz.dynawest.csvcruncher.util.logger
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.PrintStream


/*
* This was written long ago and then lost and decompiled from an old .jar of an old version, and refactored a bit.
* And then auto-converted to Kotlin.
* So please be lenient with the code below :)
*/
object App {

    @JvmStatic
    @Throws(Exception::class)
    fun mainNoExit(args: Array<String>) {
        val options = OptionsParser.parseArgs(args) ?: return

        setLogLevel(options)

        log.debug("Options: \n$options")
        Cruncher(options).crunch()
    }

    @JvmStatic
    fun main(args: Array<String>) {
        try {
            mainNoExit(args)
        }
        catch (ex: Exception) {
            val exitCode = when (ex) {
                // Known app errors, or user errors -> no stacktrace, exit with error code.
                is IllegalArgumentException,  -> 1
                is SqlSyntaxCruncherException -> 11
                is CsvCruncherException       -> 20
                // Unknown -> Print with stacktrace.
                else -> -1;
            }
            if (exitCode == -1) {
                System.err.println("" + ex.message)
                printBugReportingGuide(System.err, null)
                System.exit(exitCode)
            }
            else {
                //log.error("CSV Cruncher failed: " + ex.message, ex)
                System.err.println("CSV Cruncher failed: " + ex.message + "\n" + ex)
                System.exit(127)
            }
        }
        catch (ex: Throwable) {
            log.error("CSV Cruncher failed: " + ex.message, ex)
            System.exit(127)
        }
    }


    private fun printBanner() {
        println(""" |
                    | 
                    |     ____________    __   ______                      __             
                    |    / ____/ ___/ |  / /  / ____/______  ______  _____/ /_  ___  _____
                    |   / /    \__ \| | / /  / /   / ___/ / / / __ \/ ___/ __ \/ _ \/ ___/
                    |  / /___ ___/ /| |/ /  / /___/ /  / /_/ / / / / /__/ / / /  __/ /    
                    |  \____//____/ |___/   \____/_/   \__,_/_/ /_/\___/_/ /_/\___/_/     
                    |                                                                   
                    |
                    |""".trimMargin()
        )
    }

    internal fun printUsage(outputStream: PrintStream) {
        outputStream.println("  Usage:")
        outputStream.println("    crunch -in <input.json> [-in <input2.json> ...] -out <output.csv> [-sql <SQL>] [--<option> --...] ")
        outputStream.println("  For more, read the README.md distributed with CsvCruncher, or at: https://github.com/OndraZizka/csv-cruncher#readme")
    }

    internal fun printBugReportingGuide(outputStream: PrintStream, options: Options?) {
        if (options?.logLevel == LogLevel.OFF) return;
        outputStream.println("  If you believe this is a bug, feel free to report it at https://github.com/OndraZizka/csv-cruncher/issues")
        outputStream.println("  but please check the open tickets first.")
        if (options?.logLevel !in setOf(null, LogLevel.DEBUG, LogLevel.TRACE))
            outputStream.println("  For more info about what CsvCruncher did before the error, try --logLevel=${LogLevel.DEBUG.name}")
    }

    private fun setLogLevel(options: Options) {
        if (options.logLevel == null) return

        val configuredLevel: Level? = Level.toLevel(options.logLevel!!.name)

        val rootLogger: Logger = LoggerFactory.getLogger("root")
        val logBackLogger = rootLogger as ch.qos.logback.classic.Logger
        rootLogger.debug("Changing all loggers' level to ${configuredLevel} as per option --logLevel=${options.logLevel!!.name}. Possible levels: " + LogLevel.entries.joinToString(", "))
        logBackLogger.level = configuredLevel

        // For now, this needs to be synchronized with logback.xml, to override all specifically set there.
        for (loggerName in listOf("cz.dynawest.csvcruncher"))
            (LoggerFactory.getLogger(loggerName) as ch.qos.logback.classic.Logger).level = configuredLevel
    }


    init { printBanner() }

    private val log = logger()
}
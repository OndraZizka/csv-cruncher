package cz.dynawest.csvcruncher.app

import cz.dynawest.csvcruncher.Cruncher
import cz.dynawest.csvcruncher.util.logger
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
        log.info("Options: \n$options")
        Cruncher(options).crunch()
    }

    @JvmStatic
    fun main(args: Array<String>) {
        try {
            mainNoExit(args)
        }
        catch (ex: IllegalArgumentException) {
            println("" + ex.message)
            System.exit(1)
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

    internal fun printUsage(dest: PrintStream) {
        dest.println("  Usage:")
        dest.println("    crunch [-in] <inCSV> [<inCSV> ...] [-out] <outCSV> [--<option> --...] [-sql] <SQL>")
        /*
        dest.println("  Options:");
        dest.println("    --ignoreFirstLines[=<number>]     Ignore first N lines; the first is considered a header with column names.");
        dest.println("    --rowNumbers[=<firstNumber>]      Add an unique incrementing number as a first column.");
        dest.println("    --json[=<firstNumber>]      ");
        TODO: Copy from the README.
        */
    }

    init { printBanner() }

    private val log = logger()
}
package cz.dynawest.csvcruncher

import cz.dynawest.csvcruncher.Options.*
import cz.dynawest.csvcruncher.util.Utils.version
import cz.dynawest.csvcruncher.util.logger
import org.apache.commons.lang3.StringUtils
import java.io.PrintStream
import java.util.regex.Pattern

/*
* This was written long ago and then lost and decompiled from an old .jar of an old version, and refactored a bit.
* So please be lenient with the code below :)
*/
object App {
    private val log = logger()

    @JvmStatic
    @Throws(Exception::class)
    fun mainNoExit(args: Array<String>) {
        val options = parseArgs(args) ?: return
        log.info("Options: \n$options")
        Cruncher(options).crunch()
    }

    @JvmStatic
    @Throws(Exception::class)
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

    private fun parseArgs(args: Array<String>): Options? {

        val opt = Options()
        var relPos = -1
        var next: OptionsNext? = null
        log.debug(" Parameters: ")

        for (i in args.indices) {
            val arg = args[i]
            //System.out.println(" * " + arg);
            log.debug(" * $arg")

            // JSON output
            if (arg.startsWith("--" + JsonExportFormat.PARAM_NAME)) {
                if (arg.endsWith("=" + JsonExportFormat.ARRAY.optionValue)) opt.jsonExportFormat =
                    JsonExportFormat.ARRAY
                else opt.jsonExportFormat = JsonExportFormat.ENTRY_PER_LINE
            }
            else if (arg.startsWith("--include")) {
                require(arg.startsWith("--include=")) { "Option --include has to have a value (regular expression)." }
                val regex = StringUtils.removeStart(arg, "--include=")
                try {
                    opt.includePathsRegex = Pattern.compile(regex)
                }
                catch (ex: Exception) {
                    throw CsvCruncherException("Not a valid regex: $regex. ${ex.message}", ex)
                }
            }
            else if (arg.startsWith("--exclude")) {
                require(arg.startsWith("--exclude=")) { "Option --exclude has to have a value (regular expression)." }
                val regex = StringUtils.removeStart(arg, "--exclude=")
                try {
                    opt.excludePathsRegex = Pattern.compile(regex)
                }
                catch (ex: Exception) {
                    throw CsvCruncherException("Not a valid regex: $regex. ${ex.message}", ex)
                }
            }
            else if (arg.startsWith("--ignoreFirstLines")) {
                opt.ignoreFirstLines = 1
                if (arg.startsWith("--ignoreFirstLines=")) {
                    val numberStr = StringUtils.removeStart(arg, "--ignoreFirstLines=")
                    try {
                        val number = numberStr.toInt()
                        opt.ignoreFirstLines = number
                    }
                    catch (ex: Exception) {
                        throw CsvCruncherException("Not a valid number: $numberStr. ${ex.message}", ex)
                    }
                }
            }
            else if (arg.startsWith("--ignoreLinesMatching")) {
                require(arg.startsWith("--ignoreLinesMatching=")) { "Option --ignoreLinesMatching has to have a value (regular expression)." }
                val regex = StringUtils.removeStart(arg, "--ignoreFirstLines=")
                try {
                    opt.ignoreLineRegex = Pattern.compile(regex)
                }
                catch (ex: Exception) {
                    throw CsvCruncherException("Not a valid regex: $regex. ${ex.message}", ex)
                }
            }
            else if (arg.startsWith("--rowNumbers")) {
                opt.initialRowNumber = -1L
                if (arg.startsWith("--rowNumbers=")) {
                    val numberStr = StringUtils.removeStart(arg, "--rowNumbers=")
                    try {
                        val number = numberStr.toLong()
                        opt.initialRowNumber = number
                    }
                    catch (ex: Exception) {
                        throw CsvCruncherException("Not a valid number: $numberStr. ${ex.message}", ex)
                    }
                }
            }
            else if (arg.startsWith("--" + SortInputPaths.PARAM_SORT_INPUT_PATHS)) {
                if (arg.endsWith("--" + SortInputPaths.PARAM_SORT_INPUT_PATHS) ||
                    arg.endsWith("=" + SortInputPaths.PARAMS_ORDER.optionValue)
                ) opt.sortInputPaths = SortInputPaths.PARAMS_ORDER
                else if (arg.endsWith("=" + SortInputPaths.ALPHA.optionValue)) 
                    opt.sortInputPaths = SortInputPaths.ALPHA
                else if (arg.endsWith("=" + SortInputPaths.TIME.optionValue)) 
                    opt.sortInputPaths = SortInputPaths.TIME
                else
                    throw IllegalArgumentException("Unknown value for ${SortInputPaths.PARAM_SORT_INPUT_PATHS}: $arg Try one of ${SortInputPaths.optionValues}")
            }
            else if (arg.startsWith("--" + SortInputPaths.PARAM_SORT_FILE_GROUPS)) {
                if (arg.endsWith("--" + SortInputPaths.PARAM_SORT_FILE_GROUPS) ||
                    arg.endsWith("=" + SortInputPaths.ALPHA.optionValue)
                ) opt.sortInputFileGroups = SortInputPaths.ALPHA
                else if (arg.endsWith("=" + SortInputPaths.TIME.optionValue)) 
                    opt.sortInputFileGroups = SortInputPaths.TIME
                else if (arg.endsWith("=" + SortInputPaths.PARAMS_ORDER.optionValue)) 
                    opt.sortInputFileGroups = SortInputPaths.PARAMS_ORDER
                else
                    throw IllegalArgumentException("Unknown value for ${SortInputPaths.PARAM_SORT_FILE_GROUPS}: $arg Try one of ${SortInputPaths.optionValues}")
            }
            else if (arg.startsWith("--" + CombineInputFiles.PARAM_NAME)) {
                if (arg.endsWith("--" + CombineInputFiles.PARAM_NAME) ||
                    arg.endsWith("=" + CombineInputFiles.CONCAT.optionValue)
                ) opt.combineInputFiles = CombineInputFiles.CONCAT
                else if (arg.endsWith("=" + CombineInputFiles.INTERSECT.optionValue))
                    opt.combineInputFiles = CombineInputFiles.INTERSECT
                else if (arg.endsWith("=" + CombineInputFiles.EXCEPT.optionValue))
                    opt.combineInputFiles = CombineInputFiles.EXCEPT
                else
                    throw IllegalArgumentException("Unknown value for ${CombineInputFiles.PARAM_NAME}: $arg Try one of ${CombineInputFiles.optionValues}")

                // TODO: Do something like this instead:
                //opt.combineInputFiles = Utils.processOptionIfMatches(arg, Options.CombineInputFiles.class, Options.CombineInputFiles.CONCAT);
                // Or move it to the respective enum class.
                // Enum<Options.CombineDirectories> val = Options.CombineDirectories.COMBINE_ALL_FILES;
            }
            else if (arg.startsWith("--" + CombineDirectories.PARAM_NAME)) {
                // Sorted from most fine-grained to least.
                if (arg.endsWith("=" + CombineDirectories.COMBINE_PER_EACH_DIR.optionValue))
                    opt.combineDirs = CombineDirectories.COMBINE_PER_EACH_DIR
                else if (arg.endsWith("=" + CombineDirectories.COMBINE_PER_INPUT_SUBDIR.optionValue))
                    opt.combineDirs = CombineDirectories.COMBINE_PER_INPUT_SUBDIR
                else if (arg.endsWith("=" + CombineDirectories.COMBINE_PER_INPUT_DIR.optionValue))
                    opt.combineDirs = CombineDirectories.COMBINE_PER_INPUT_DIR
                else if (arg.endsWith("=" + CombineDirectories.COMBINE_ALL_FILES.optionValue))
                    opt.combineDirs = CombineDirectories.COMBINE_ALL_FILES
                else if (arg == "--" + CombineDirectories.PARAM_NAME)
                    opt.combineDirs = CombineDirectories.COMBINE_ALL_FILES
                else
                    throw IllegalArgumentException("Unknown value for ${CombineDirectories.PARAM_NAME}: $arg Try one of ${CombineDirectories.optionValues}")
            }
            else if ("--queryPerInputSubpart" == arg) {
                opt.queryPerInputSubpart = true
            }
            else if ("-in" == arg) {
                next = OptionsNext.IN
            }
            else if ("--skipNonReadable" == arg) {
                opt.skipNonReadable = true
            }
            else if ("--keepWorkFiles" == arg) {
                opt.keepWorkFiles = true
            }
            else if ("-out" == arg) {
                next = OptionsNext.OUT
                relPos = 2
            }
            else if ("-sql" == arg) {
                next = OptionsNext.SQL
                relPos = 3
            }
            else if ("-db" == arg) {
                next = OptionsNext.DBPATH
            }
            else if ("-v" == arg || "--version" == arg) {
                val version = version
                println(" CSV Cruncher version $version")
                return null
            }
            else if ("-h" == arg || "--help" == arg) {
                val version = version
                println(" CSV Cruncher version $version")
                printUsage(System.out)
                return null
            }
            else if (arg.startsWith("--overwrite")) {
                opt.overwrite = true
            }
            else if (arg.startsWith("-")) {
                val msg = "Unknown parameter: $arg"
                println("ERROR: $msg")
                throw IllegalArgumentException(msg)
            }
            else {
                if (next != null) {
                    when (next) {
                        OptionsNext.IN -> {
                            opt.inputPaths!!.add(arg)
                            relPos = Math.max(relPos, 1)
                            continue
                        }
                        OptionsNext.OUT -> {
                            opt.outputPathCsv = arg
                            relPos = Math.max(relPos, 2)
                            continue
                        }
                        OptionsNext.SQL -> {
                            opt.sql = arg
                            relPos = Math.max(relPos, 3)
                            continue
                        }
                        OptionsNext.DBPATH -> {
                            opt.dbPath = arg
                            continue
                        }
                    }
                }
                ++relPos
                if (relPos == 0) {
                    opt.inputPaths!!.add(arg)
                }
                else if (relPos == 1) {
                    opt.outputPathCsv = arg
                }
                else {
                    if (relPos != 2) {
                        printUsage(System.out)
                        throw IllegalArgumentException("Wrong arguments. Usage: crunch [-in] <inCSV> [-out] <outCSV> [-sql] <SQL> ...")
                    }
                    opt.sql = arg
                }
            }
        }

        // HSQLDB bug, see https://stackoverflow.com/questions/52708378/hsqldb-insert-into-select-null-from-leads-to-duplicate-column-name
        if (opt.initialRowNumber != null && opt.sql != null) {
            val itsForSure = opt.sql!!.matches(".*SELECT +\\*.*|.*[^.]\\* +FROM .*".toRegex())
            if (itsForSure || opt.sql!!.matches(".*SELECT.*[^.]\\* .*FROM.*".toRegex())) {
                val msg = """
    WARNING! It looks like you use --rowNumbers with `SELECT *`.
    Due to a bug in HSQLDB, this causes an error 'duplicate column name in derived table'.
    Use table-qualified way: `SELECT myTable.*`"""
                if (itsForSure) {
                    log.error("\n$msg\n\n")
                    throw IllegalArgumentException(msg)
                } else {
                    val notSure = "\n    (This detection is not reliable so the program will continue, but likely fail.)"
                    log.warn("\n$msg$notSure\n\n")
                }
            }
        }
        return if (!opt.isFilled) {
            printUsage(System.out)
            throw IllegalArgumentException("Not enough arguments. Usage: crunch [-in] <inCSV> [-out] <outCSV> [-sql] <SQL> ...")
        } else {
            opt
        }
    }

    private fun printBanner() {
        println(
                """

   ____________    __   ______                      __             
  / ____/ ___/ |  / /  / ____/______  ______  _____/ /_  ___  _____
 / /    \__ \| | / /  / /   / ___/ / / / __ \/ ___/ __ \/ _ \/ ___/
/ /___ ___/ /| |/ /  / /___/ /  / /_/ / / / / /__/ / / /  __/ /    
\____//____/ |___/   \____/_/   \__,_/_/ /_/\___/_/ /_/\___/_/     
                                                                   

"""
        )
    }

    private fun printUsage(dest: PrintStream) {
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

    private enum class OptionsNext {
        IN, OUT, SQL, DBPATH
    }

    init {
        printBanner()
    }
}
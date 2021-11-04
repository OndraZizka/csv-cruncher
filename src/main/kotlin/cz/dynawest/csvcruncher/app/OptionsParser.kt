package cz.dynawest.csvcruncher.app

import cz.dynawest.csvcruncher.CsvCruncherException
import cz.dynawest.csvcruncher.util.Utils
import cz.dynawest.csvcruncher.util.logger
import org.apache.commons.lang3.StringUtils
import java.util.regex.Pattern
import kotlin.math.max

object OptionsParser {

    fun parseArgs(args: Array<String>): Options? {

        val opt = Options()
        var relPos = -1
        var next: OptionsCurrentContext? = null
        log.debug(" Parameters: ")

        for (i in args.indices) {
            val arg = args[i]
            log.debug(" * $arg")

            // JSON output
            if (arg.startsWith("--" + Options.JsonExportFormat.PARAM_NAME)) {
                if (arg.endsWith("=" + Options.JsonExportFormat.ARRAY.optionValue)) opt.jsonExportFormat =
                    Options.JsonExportFormat.ARRAY
                else opt.jsonExportFormat = Options.JsonExportFormat.ENTRY_PER_LINE
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
            else if (arg.startsWith("--" + Options.SortInputPaths.PARAM_SORT_INPUT_PATHS)) {
                if (arg.endsWith("--" + Options.SortInputPaths.PARAM_SORT_INPUT_PATHS) ||
                    arg.endsWith("=" + Options.SortInputPaths.PARAMS_ORDER.optionValue))
                    opt.sortInputPaths = Options.SortInputPaths.PARAMS_ORDER
                else if (arg.endsWith("=" + Options.SortInputPaths.ALPHA.optionValue))
                    opt.sortInputPaths = Options.SortInputPaths.ALPHA
                else if (arg.endsWith("=" + Options.SortInputPaths.TIME.optionValue))
                    opt.sortInputPaths = Options.SortInputPaths.TIME
                else
                    throw IllegalArgumentException("Unknown value for ${Options.SortInputPaths.PARAM_SORT_INPUT_PATHS}: $arg Try one of ${Options.SortInputPaths.optionValues}")
            }
            else if (arg.startsWith("--" + Options.SortInputPaths.PARAM_SORT_FILE_GROUPS)) {
                if (arg.endsWith("--" + Options.SortInputPaths.PARAM_SORT_FILE_GROUPS) ||
                    arg.endsWith("=" + Options.SortInputPaths.ALPHA.optionValue))
                    opt.sortInputFileGroups = Options.SortInputPaths.ALPHA
                else if (arg.endsWith("=" + Options.SortInputPaths.TIME.optionValue))
                    opt.sortInputFileGroups = Options.SortInputPaths.TIME
                else if (arg.endsWith("=" + Options.SortInputPaths.PARAMS_ORDER.optionValue))
                    opt.sortInputFileGroups = Options.SortInputPaths.PARAMS_ORDER
                else
                    throw IllegalArgumentException("Unknown value for ${Options.SortInputPaths.PARAM_SORT_FILE_GROUPS}: $arg Try one of ${Options.SortInputPaths.optionValues}")
            }
            else if (arg.startsWith("--" + Options.CombineInputFiles.PARAM_NAME)) {
                if (arg.endsWith("--" + Options.CombineInputFiles.PARAM_NAME) ||
                    arg.endsWith("=" + Options.CombineInputFiles.CONCAT.optionValue))
                    opt.combineInputFiles = Options.CombineInputFiles.CONCAT
                else if (arg.endsWith("=" + Options.CombineInputFiles.INTERSECT.optionValue))
                    opt.combineInputFiles = Options.CombineInputFiles.INTERSECT
                else if (arg.endsWith("=" + Options.CombineInputFiles.EXCEPT.optionValue))
                    opt.combineInputFiles = Options.CombineInputFiles.EXCEPT
                else
                    throw IllegalArgumentException("Unknown value for ${Options.CombineInputFiles.PARAM_NAME}: $arg Try one of ${Options.CombineInputFiles.optionValues}")

                // TODO: Do something like this instead:
                //opt.combineInputFiles = Utils.processOptionIfMatches(arg, Options.CombineInputFiles.class, Options.CombineInputFiles.CONCAT);
                // Or move it to the respective enum class.
                // Enum<Options.CombineDirectories> val = Options.CombineDirectories.COMBINE_ALL_FILES;
            }
            else if (arg.startsWith("--" + Options.CombineDirectories.PARAM_NAME)) {
                // Sorted from most fine-grained to least.
                if (arg.endsWith("=" + Options.CombineDirectories.COMBINE_PER_EACH_DIR.optionValue))
                    opt.combineDirs = Options.CombineDirectories.COMBINE_PER_EACH_DIR
                else if (arg.endsWith("=" + Options.CombineDirectories.COMBINE_PER_INPUT_SUBDIR.optionValue))
                    opt.combineDirs = Options.CombineDirectories.COMBINE_PER_INPUT_SUBDIR
                else if (arg.endsWith("=" + Options.CombineDirectories.COMBINE_PER_INPUT_DIR.optionValue))
                    opt.combineDirs = Options.CombineDirectories.COMBINE_PER_INPUT_DIR
                else if (arg.endsWith("=" + Options.CombineDirectories.COMBINE_ALL_FILES.optionValue))
                    opt.combineDirs = Options.CombineDirectories.COMBINE_ALL_FILES
                else if (arg == "--" + Options.CombineDirectories.PARAM_NAME)
                    opt.combineDirs = Options.CombineDirectories.COMBINE_ALL_FILES
                else
                    throw IllegalArgumentException("Unknown value for ${Options.CombineDirectories.PARAM_NAME}: $arg Try one of ${Options.CombineDirectories.optionValues}")
            }
            else if ("--queryPerInputSubpart" == arg) {
                opt.queryPerInputSubpart = true
            }
            else if ("-in" == arg) {
                next = OptionsCurrentContext.IN
            }
            else if ("--skipNonReadable" == arg) {
                opt.skipNonReadable = true
            }
            else if ("--keepWorkFiles" == arg) {
                opt.keepWorkFiles = true
            }
            else if ("-out" == arg) {
                next = OptionsCurrentContext.OUT
                relPos = 2
            }
            else if ("-sql" == arg) {
                next = OptionsCurrentContext.SQL
                relPos = 3
            }
            else if ("-db" == arg) {
                next = OptionsCurrentContext.DBPATH
            }
            else if ("-v" == arg || "--version" == arg) {
                val version = Utils.version
                println(" CSV Cruncher version $version")
                return null
            }
            else if ("-h" == arg || "--help" == arg) {
                val version = Utils.version
                println(" CSV Cruncher version $version")
                App.printUsage(System.out)
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
                        OptionsCurrentContext.IN -> {
                            opt.inputPaths!!.add(arg)
                            relPos = max(relPos, 1)
                            continue
                        }
                        OptionsCurrentContext.OUT -> {
                            opt.outputPathCsv = arg
                            relPos = max(relPos, 2)
                            continue
                        }
                        OptionsCurrentContext.SQL -> {
                            opt.sql = arg
                            relPos = max(relPos, 3)
                            continue
                        }
                        OptionsCurrentContext.DBPATH -> {
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
                        App.printUsage(System.out)
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
            App.printUsage(System.out)
            throw IllegalArgumentException("Not enough arguments. Usage: crunch [-in] <inCSV> [-out] <outCSV> [-sql] <SQL> ...")
        } else {
            opt
        }
    }

    enum class OptionsCurrentContext {
        IN, OUT, SQL, DBPATH
    }

    private val log = logger()
}
package cz.dynawest.csvcruncher.app

import cz.dynawest.csvcruncher.CsvCruncherException
import cz.dynawest.csvcruncher.app.Options.*
import cz.dynawest.csvcruncher.util.Utils
import cz.dynawest.csvcruncher.util.logger
import org.apache.commons.lang3.StringUtils
import java.util.regex.Pattern
import kotlin.math.max

object OptionsParser {

    fun parseArgs(args: Array<String>): Options? {

        val options = Options()
        var relativePosition = -1
        var next: OptionsCurrentContext? = null
        log.debug(" Parameters: ")

        for (i in args.indices) {
            val arg = args[i]
            log.debug(" * $arg")

            // JSON output
            if (arg.startsWith("--" + JsonExportFormat.PARAM_NAME)) {
                if (arg.endsWith("=" + JsonExportFormat.ARRAY.optionValue))
                    options.jsonExportFormat = JsonExportFormat.ARRAY
                else options.jsonExportFormat = JsonExportFormat.ENTRY_PER_LINE
            }
            else if (arg.startsWith("--include")) {
                require(arg.startsWith("--include=")) { "Option --include has to have a value (regular expression)." }
                val regex = StringUtils.removeStart(arg, "--include=")
                try {
                    options.includePathsRegex = Pattern.compile(regex)
                }
                catch (ex: Exception) {
                    throw CsvCruncherException("Not a valid regex: $regex. ${ex.message}", ex)
                }
            }
            else if (arg.startsWith("--exclude")) {
                require(arg.startsWith("--exclude=")) { "Option --exclude has to have a value (regular expression)." }
                val regex = StringUtils.removeStart(arg, "--exclude=")
                try {
                    options.excludePathsRegex = Pattern.compile(regex)
                }
                catch (ex: Exception) {
                    throw CsvCruncherException("Not a valid regex: $regex. ${ex.message}", ex)
                }
            }
            else if (arg.startsWith("--ignoreFirstLines")) {
                options.ignoreFirstLines = 1
                if (arg.startsWith("--ignoreFirstLines=")) {
                    val numberStr = StringUtils.removeStart(arg, "--ignoreFirstLines=")
                    try {
                        val number = numberStr.toInt()
                        options.ignoreFirstLines = number
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
                    options.ignoreLineRegex = Pattern.compile(regex)
                }
                catch (ex: Exception) {
                    throw CsvCruncherException("Not a valid regex: $regex. ${ex.message}", ex)
                }
            }
            else if (arg.startsWith("--rowNumbers")) {
                options.initialRowNumber = -1L
                if (arg.startsWith("--rowNumbers=")) {
                    val numberStr = StringUtils.removeStart(arg, "--rowNumbers=")
                    try {
                        val number = numberStr.toLong()
                        options.initialRowNumber = number
                    }
                    catch (ex: Exception) {
                        throw CsvCruncherException("Not a valid number: $numberStr. ${ex.message}", ex)
                    }
                }
            }
            else if (arg.startsWith("--" + SortInputPaths.PARAM_SORT_INPUT_PATHS)) {
                if (arg.endsWith("--" + SortInputPaths.PARAM_SORT_INPUT_PATHS) ||
                    arg.endsWith("=" + SortInputPaths.PARAMS_ORDER.optionValue))
                    options.sortInputPaths = SortInputPaths.PARAMS_ORDER
                else if (arg.endsWith("=" + SortInputPaths.ALPHA.optionValue))
                    options.sortInputPaths = SortInputPaths.ALPHA
                else if (arg.endsWith("=" + SortInputPaths.TIME.optionValue))
                    options.sortInputPaths = SortInputPaths.TIME
                else
                    throw IllegalArgumentException("Unknown value for ${SortInputPaths.PARAM_SORT_INPUT_PATHS}: $arg Try one of ${SortInputPaths.optionValues}")
            }
            else if (arg.startsWith("--" + SortInputPaths.PARAM_SORT_FILE_GROUPS)) {
                if (arg.endsWith("--" + SortInputPaths.PARAM_SORT_FILE_GROUPS) ||
                    arg.endsWith("=" + SortInputPaths.ALPHA.optionValue))
                    options.sortInputFileGroups = SortInputPaths.ALPHA
                else if (arg.endsWith("=" + SortInputPaths.TIME.optionValue))
                    options.sortInputFileGroups = SortInputPaths.TIME
                else if (arg.endsWith("=" + SortInputPaths.PARAMS_ORDER.optionValue))
                    options.sortInputFileGroups = SortInputPaths.PARAMS_ORDER
                else
                    throw IllegalArgumentException("Unknown value for ${SortInputPaths.PARAM_SORT_FILE_GROUPS}: $arg Try one of ${SortInputPaths.optionValues}")
            }
            else if (arg.startsWith("--" + CombineInputFiles.PARAM_NAME)) {
                if (arg.endsWith("--" + CombineInputFiles.PARAM_NAME) ||
                    arg.endsWith("=" + CombineInputFiles.CONCAT.optionValue))
                    options.combineInputFiles = CombineInputFiles.CONCAT
                else if (arg.endsWith("=" + CombineInputFiles.INTERSECT.optionValue))
                    options.combineInputFiles = CombineInputFiles.INTERSECT
                else if (arg.endsWith("=" + CombineInputFiles.EXCEPT.optionValue))
                    options.combineInputFiles = CombineInputFiles.EXCEPT
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
                    options.combineDirs = CombineDirectories.COMBINE_PER_EACH_DIR
                else if (arg.endsWith("=" + CombineDirectories.COMBINE_PER_INPUT_SUBDIR.optionValue))
                    options.combineDirs = CombineDirectories.COMBINE_PER_INPUT_SUBDIR
                else if (arg.endsWith("=" + CombineDirectories.COMBINE_PER_INPUT_DIR.optionValue))
                    options.combineDirs = CombineDirectories.COMBINE_PER_INPUT_DIR
                else if (arg.endsWith("=" + CombineDirectories.COMBINE_ALL_FILES.optionValue))
                    options.combineDirs = CombineDirectories.COMBINE_ALL_FILES
                else if (arg == "--" + CombineDirectories.PARAM_NAME)
                    options.combineDirs = CombineDirectories.COMBINE_ALL_FILES
                else
                    throw IllegalArgumentException("Unknown value for ${CombineDirectories.PARAM_NAME}: $arg Try one of ${CombineDirectories.optionValues}")
            }
            else if ("--queryPerInputSubpart" == arg) {
                options.queryPerInputSubpart = true
            }
            else if ("-in" == arg) {
                next = OptionsCurrentContext.IN
            }
            else if ("--skipNonReadable" == arg) {
                options.skipNonReadable = true
            }
            else if ("--keepWorkFiles" == arg) {
                options.keepWorkFiles = true
            }
            else if ("-out" == arg) {
                next = OptionsCurrentContext.OUT
                relativePosition = 2
            }
            else if ("-sql" == arg) {
                next = OptionsCurrentContext.SQL
                relativePosition = 3
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
                options.overwrite = true
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
                            options.inputPaths!!.add(arg)
                            relativePosition = max(relativePosition, 1)
                            continue
                        }
                        OptionsCurrentContext.OUT -> {
                            options.outputPathCsv = arg
                            relativePosition = max(relativePosition, 2)
                            continue
                        }
                        OptionsCurrentContext.SQL -> {
                            options.sql = arg
                            relativePosition = max(relativePosition, 3)
                            continue
                        }
                        OptionsCurrentContext.DBPATH -> {
                            options.dbPath = arg
                            continue
                        }
                    }
                }
                ++relativePosition

                when (relativePosition) {
                    0 -> options.inputPaths!!.add(arg)
                    1 -> options.outputPathCsv = arg
                    else -> {
                        if (relativePosition != 2) {
                            App.printUsage(System.out)
                            throw IllegalArgumentException("Wrong arguments. Usage: crunch [-in] <inCSV> [-out] <outCSV> [-sql] <SQL> ...")
                        }
                        options.sql = arg
                    }
                }
            }
        }

        // HSQLDB bug, see https://stackoverflow.com/questions/52708378/hsqldb-insert-into-select-null-from-leads-to-duplicate-column-name
        if (options.initialRowNumber != null && options.sql != null) {
            val itsForSure = options.sql!!.matches(".*SELECT +\\*.*|.*[^.]\\* +FROM .*".toRegex())
            if (itsForSure || options.sql!!.matches(".*SELECT.*[^.]\\* .*FROM.*".toRegex())) {
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
        return if (!options.isFilled) {
            App.printUsage(System.out)
            throw IllegalArgumentException("Not enough arguments. Usage: crunch [-in] <inCSV> [-out] <outCSV> [-sql] <SQL> ...")
        } else {
            options
        }
    }

    enum class OptionsCurrentContext {
        IN, OUT, SQL, DBPATH
    }

    private val log = logger()
}
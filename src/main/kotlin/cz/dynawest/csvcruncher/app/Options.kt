package cz.dynawest.csvcruncher.app

import ch.qos.logback.classic.Level
import cz.dynawest.csvcruncher.CrucherConfigException
import cz.dynawest.csvcruncher.Cruncher
import cz.dynawest.csvcruncher.app.OptionsEnums.CombineDirectories.COMBINE_PER_INPUT_SUBDIR
import cz.dynawest.csvcruncher.app.csvRegexParts.columnNames
import cz.dynawest.csvcruncher.util.logger
import java.io.FileNotFoundException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.regex.Pattern
import kotlin.io.path.extension

class ImportArgument {
    var path: Path? = null
    var alias: String? = null
    var format: DataFormat? = null
    var formatFrom: DataFormatFrom = DataFormatFrom.ASSUMED
    var itemsPathInTree: String = "/"
    var indexed: List<String> = emptyList()

    // Overrides for the defaults. Not implemented yet.
    var includePathsRegex: Pattern? = null
    var excludePathsRegex: Pattern? = null
    var ignoreFirstLines: Int? = null
    var ignoreLineRegex: Pattern? = null
    /** If not null, add a 1st column with unique incrementing numbers, starting at this value. */
    var initialRowNumber: Long? = null
    var sortInputFileGroups: OptionsEnums.SortInputPaths? = null
    var combineInputFiles: OptionsEnums.CombineInputFiles? = null
    var jsonExportFormat: OptionsEnums.JsonExportFormat? = null

    override fun toString(): String {
        return "(${alias?:"no alias"}) [$format from $formatFrom] ${initialRowNumber?:""} <- $path ${itemsPathInTree}"
    }
}


class ExportArgument {
    var sqlQuery: String? = null
    /** The file to export the CSV table to. May be an intermediate storage. If null, will be replaced with a temporary file path. */
    var path: Path? = null
    var targetType: TargetType = TargetType.STDOUT
    var alias: String? = null
    var formats: MutableSet<DataFormat> = mutableSetOf(DataFormat.CSV)


    override fun toString(): String {
        val outputPathOrSpecial = when (targetType) {TargetType.STDOUT -> "<STDOUT>"; else -> path }
        return "(${alias ?: "no alias"}) $formats -> $outputPathOrSpecial\n\t\t\"${ sqlQuery ?: "(no SQL query)" }\""
    }

    enum class TargetType(
        val specialOptionValue: String?,
        val replacementPath: ((ExportArgument) -> Path)?
    ) {
        /** This export was explicitly requested to go to STDOUT (versus just a missing path). */
        STDOUT("-", { exArg -> java.io.File.createTempFile("CsvCrunch-", exArg.formats.firstOrNull()?.suffixes?.firstOrNull() ?: ".csv").toPath() } ),
        FILE(null, { it.path ?: throw IllegalStateException("TargetType == FILE, but theres no path: $it") }),
    }
}

data class InitSqlArgument (
    var path: Path
)


internal object csvRegexParts {
    const val columnName = "[\\w-_. ]+"
    const val quotedOrUnquoted = """("$columnName")|($columnName)"""
    const val quotedOrUnquotedSpaced = """(\s*$quotedOrUnquoted\s*)"""
    const val columnNames = """^\s*#?\s*$quotedOrUnquotedSpaced(,$quotedOrUnquotedSpaced)*,?\n?"""
}

enum class DataFormat (val suffixes: List<String>, val beginningLineRegex: Regex) {

    // Auto-detection:  A comma-separated list of quoted or unquoted column names,
    //                  optionally starting with #, and optional trailing comma.
    CSV(listOf(".csv"), columnNames.toRegex()),

    // Auto-detection:  Starts with { or [ followed by " or \n.
    JSON(listOf(".json"), "^\\s*[{\\[]\\s*[\"\\n]*.*".toRegex());

    val suffix: String get() = suffixes.first()

    companion object {
        fun fromExtension(path: Path): DataFormat? {
            return path.last().extension.ifEmpty { null }
                ?.let { extension -> DataFormat.entries.find { ".$extension" in it.suffixes } }
        }

        /**
         * Detects from a string what the file type likely is.
         * For simplicity, it operates on String, which should be the first line for CSV,
         * and the first non-blank few characters for JSON.
         *
         * We could also use com.fasterxml.jackson.core.format.DataFormatDetector.
         */
        fun detectFormat(beginning: String): DataFormat?
            = DataFormat.entries.find { it.beginningLineRegex.matches(beginning) }
    }
}

enum class DataFormatFrom { PARAM, FILE_SUFFIX, DETECTOR, ASSUMED }

enum class LogLevel (val logbackLevel: Level) {
    TRACE(Level.TRACE), DEBUG(Level.DEBUG), INFO(Level.INFO), WARN(Level.WARN), ERROR(Level.ERROR), OFF(Level.OFF)
}

enum class ExitCleanupStrategy { DELETE, KEEP, COMPRESS }


class Options {

    val initSqlArguments = mutableListOf<InitSqlArgument>()
    val importArguments = mutableListOf<ImportArgument>()
    val exportArguments = mutableListOf<ExportArgument>()

    var includePathsRegex: Pattern? = null
    var excludePathsRegex: Pattern? = null
    var skipNonReadable = false

    /**
     * The input files in some file group may have different structure.
     * Normally, that causes a processing error and fails.
     * With this option, it is possible to handle such cases, but the SQL needs to be generic.
     * For each different structure, a table is created and processed separatedly.
     * In this mode, other tables may not be available under the expected names.
     * The SQL may reliably referer only to the processed table as "$table".
     */
    var queryPerInputSubpart = false
    var overwrite = false
    var dbPath: String? = null
    var dbDirOnExit: ExitCleanupStrategy = ExitCleanupStrategy.KEEP
    var keepWorkFiles = false
    var sortInputPaths = OptionsEnums.SortInputPaths.PARAMS_ORDER
    var combineDirs = OptionsEnums.CombineDirectories.COMBINE_PER_EACH_DIR
    var logLevel: LogLevel? = null

    // The options below are applicable to individual Imports. These serve as defaults.
    var ignoreFirstLines = 1
    var ignoreLineRegex: Pattern? = null
    var initialRowNumber: Long? = null
    var sortInputFileGroups = OptionsEnums.SortInputPaths.ALPHA
    var combineInputFiles = OptionsEnums.CombineInputFiles.NONE
    var jsonExportFormat = OptionsEnums.JsonExportFormat.NONE

    val isFilled: Boolean
        get() = importArguments.isNotEmpty() && exportArguments.isNotEmpty()

    fun newImportArgument() = ImportArgument().also { importArguments.add(it) }
    fun newExportArgument() = ExportArgument().also { exportArguments.add(it) }

    val mainOutputDir: Path
        get() {
            val outPath: Path = exportArguments.firstNotNullOf { it.path }
            return if (outPath.toFile().isFile) outPath.parent else outPath
        }

    fun validateAndApplyDefaults() {
        require(importArguments.isNotEmpty()) { "No inputs - use `-in <pathToDataFile>`." }

        // SQL may be omitted if there is a request to combine files or convert to JSON. Otherwise it would be a no-op.
        if (exportArguments.isEmpty() || exportArguments.all { it.sqlQuery == null }) {
            log.debug(" -sql is not set, using default: " + Cruncher.DEFAULT_SQL)

            if (exportArguments.isEmpty())  exportArguments.add(ExportArgument())
            exportArguments.forEach { it.sqlQuery = Cruncher.DEFAULT_SQL }
        }

        //require(exportArguments.all { it.path != null }) { "Some exports have no path - use `-out <path>`.\n$this" }

        require(importArguments.all { it.path != null }) { "Some imports have no path - use `-in <path>`.\n$this" }
        importArguments.filter { !it.path!!.toFile().exists() }.takeIf { it.isNotEmpty() }
            ?.let { throw FileNotFoundException("Import files do not exist:"
                    + it.joinToString { "\n  * ${it.alias?.let { "\"$it\" at " } ?: ""} ${it.path}" }) }

        if (queryPerInputSubpart){
            exportArguments.filter { !it.sqlQuery!!.contains(Cruncher.SQL_TABLE_PLACEHOLDER) } .takeIf { it.isNotEmpty() }
                ?.let {
                    val msg = "--queryPerInputSubpart is enabled, but the SQL is not generic (does not use ${Cruncher.SQL_TABLE_PLACEHOLDER})," +
                        " which doesn't make sense:" + it.map { "\n  * ${it.alias} ${it.path}" }
                    throw CrucherConfigException(msg)
                }
        }

        if (COMBINE_PER_INPUT_SUBDIR == combineDirs) {
            importArguments
                .filter { it.path != null && Paths.get(it.path!!.toUri()).toFile().isFile }
                .takeIf { it.isNotEmpty() }
                ?.let {
                    val msg =
                        "If using ${COMBINE_PER_INPUT_SUBDIR.optionName}=${COMBINE_PER_INPUT_SUBDIR.optionValue}, all inputs must be directories; these are files:" + it.map {"\n  * ${it.alias} ${it.path}"}
                    throw CrucherConfigException(msg)
                }
        }

    }

    override fun toString(): String {
        return """  |    imports: ${importArguments.joinToString { "\n|      * $it" }}
                    |    exports: ${exportArguments.joinToString { "\n|      * $it" }}
                    |    initSql: ${initSqlArguments.joinToString { "\n|      * $it" }}
                    |    dbPath: ${dbPath}
                    |    includePathsRegex: ${includePathsRegex}
                    |    excludePathsRegex: ${excludePathsRegex}
                    |    queryPerInputSubpart: ${queryPerInputSubpart}
                    |    overwrite: ${overwrite}
                    |    ignoreLineRegex: ${ignoreLineRegex}
                    |    ignoreFirstLines: ${ignoreFirstLines}
                    |    sortInputPaths: ${sortInputPaths}
                    |    sortInputFileGroups: ${sortInputFileGroups}
                    |    combineInputFiles: ${combineInputFiles}
                    |    combineDirs: ${combineDirs}
                    |    initialRowNumber: ${initialRowNumber}
                    |    jsonExportFormat: ${jsonExportFormat}
                    |    skipNonReadable: ${skipNonReadable}
                    |    logLevel: ${logLevel}"""
            .trimMargin()
    }

    companion object { private val log = logger() }
}
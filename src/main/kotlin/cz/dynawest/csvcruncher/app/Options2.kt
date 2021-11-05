package cz.dynawest.csvcruncher

import cz.dynawest.csvcruncher.app.Options
import cz.dynawest.csvcruncher.app.Options.CombineDirectories.COMBINE_PER_INPUT_SUBDIR
import cz.dynawest.csvcruncher.util.logger
import java.io.FileNotFoundException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.regex.Pattern

class ImportArgument {
    var path: Path? = null
    var alias: String? = null
    var format: Format = Format.CSV

    // Overrides for the defaults. Not implemented yet.
    var includePathsRegex: Pattern? = null
    var excludePathsRegex: Pattern? = null
    var ignoreFirstLines: Int? = null
    var ignoreLineRegex: Pattern? = null
    /** If not null, add a 1st column with unique incrementing numbers, starting at this value. */
    var initialRowNumber: Long? = null
    var sortInputFileGroups: Options.SortInputPaths? = null
    var combineInputFiles: Options.CombineInputFiles? = null
    var jsonExportFormat: Options.JsonExportFormat? = null

    override fun toString(): String {
        return "(${alias?:"no alias"}) [$format] ${initialRowNumber?:""} <- $path "
    }
}


class ExportArgument {
    var sqlQuery: String? = null
    var path: Path? = null
    var alias: String? = null
    val formats: MutableSet<Format> = mutableSetOf(Format.CSV)

    override fun toString(): String {
        return "(${alias?:"no alias"}) $formats \"$sqlQuery\" -> $path"
    }
}

enum class Format {
    CSV,
    JSON
}

class Options2 {

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
    var keepWorkFiles = false
    var sortInputPaths = Options.SortInputPaths.PARAMS_ORDER
    var combineDirs = Options.CombineDirectories.COMBINE_PER_EACH_DIR

    // The options below are applicable to individual Imports. These serve as defaults.
    var ignoreFirstLines = 1
    var ignoreLineRegex: Pattern? = null
    var initialRowNumber: Long? = null
    var sortInputFileGroups = Options.SortInputPaths.ALPHA
    var combineInputFiles = Options.CombineInputFiles.NONE
    var jsonExportFormat = Options.JsonExportFormat.NONE

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

        require(exportArguments.all { it.path != null }) { "Some exports have no path - use `-out <path>`.\n$this" }
        require(importArguments.all { it.path != null }) { "Some imports have no path - use `-in <path>`.\n$this" }
        importArguments.filter { !it.path!!.toFile().exists() }.takeIf { it.isNotEmpty() }
            ?.let { throw FileNotFoundException("Import files do not exist:" + it.map {"\n  * ${it.alias} ${it.path}"}) }

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
        return """  |    imports: ${importArguments.map { "\n|      * $it" }.joinToString() }
                    |    exports: ${exportArguments.map { "\n|      * $it" }.joinToString() }
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
                    |    skipNonReadable: ${skipNonReadable}""".trimMargin()
    }

    companion object { private val log = logger() }
}
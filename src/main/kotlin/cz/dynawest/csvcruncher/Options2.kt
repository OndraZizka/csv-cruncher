package cz.dynawest.csvcruncher

import cz.dynawest.csvcruncher.app.Options
import java.nio.file.Path
import java.util.regex.Pattern

class ImportArgument {
    var path: Path? = null
    val alias: String? = null
    val format: Format = Format.CSV

    // Overrides for the defaults. Not implemented yet.
    var includePathsRegex: Pattern? = null
    var excludePathsRegex: Pattern? = null
    var ignoreFirstLines: Int? = null
    var ignoreLineRegex: Pattern? = null
    var initialRowNumber: Long? = null
    var sortInputFileGroups: Options.SortInputPaths? = null
    var combineInputFiles: Options.CombineInputFiles? = null
    var jsonExportFormat: Options.JsonExportFormat? = null
}


class ExportArgument {
    val sqlQuery: String? = null
    var path: Path? = null
    val format: Set<Format> = setOf(Format.CSV)
}

enum class Format {
    CSV,
    JSON
}

class Options2 {

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

}
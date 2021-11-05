package cz.dynawest.csvcruncher.app

import cz.dynawest.csvcruncher.Cruncher
import cz.dynawest.csvcruncher.Cruncher.Companion.SQL_TABLE_PLACEHOLDER
import cz.dynawest.csvcruncher.util.logger
import org.apache.commons.lang3.EnumUtils
import org.apache.commons.lang3.StringUtils
import java.io.File
import java.io.FileNotFoundException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.function.Function
import java.util.regex.Pattern
import java.util.stream.Collectors

class Options {
    var inputPaths: MutableList<String?>? = ArrayList()
    var includePathsRegex: Pattern? = null
    var excludePathsRegex: Pattern? = null
    var skipNonReadable = false
    var sql: String? = null
    var outputPathCsv: String? = null

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
    var ignoreFirstLines = 1
    var ignoreLineRegex: Pattern? = null
    var initialRowNumber: Long? = null
    var sortInputPaths = SortInputPaths.PARAMS_ORDER
    var sortInputFileGroups = SortInputPaths.ALPHA
    var combineInputFiles = CombineInputFiles.NONE
    var combineDirs = CombineDirectories.COMBINE_PER_EACH_DIR
    var jsonExportFormat = JsonExportFormat.NONE

    val isFilled: Boolean
        get() = inputPaths != null && outputPathCsv != null

    @Throws(FileNotFoundException::class)
    fun validate() {
        require(!(inputPaths == null || inputPaths!!.isEmpty())) { " -in is not set." }

        // SQL may be omitted if there is a request to combine files or convert to JSON. Otherwise it would be a no-op.
        if (sql == null) {
            log.debug(" -sql is not set, using default: " + Cruncher.DEFAULT_SQL)
            sql = Cruncher.DEFAULT_SQL
        }
        requireNotNull(outputPathCsv) { " -out is not set." }
        for (path in inputPaths!!) {
            val ex = File(path)
            if (!ex.exists()) throw FileNotFoundException("CSV file not found: " + ex.path)
        }
        if (queryPerInputSubpart && !StringUtils.isBlank(sql) && !sql!!.contains(SQL_TABLE_PLACEHOLDER)) {
            val msg =
                "queryPerInputSubpart is enabled, but the SQL is not generic (does not use $SQL_TABLE_PLACEHOLDER), which doesn't make sense."
            throw IllegalArgumentException(msg)
        }
        if (CombineDirectories.COMBINE_PER_INPUT_SUBDIR == combineDirs) {
            for (inputPath in inputPaths!!) {
                if (Paths.get(inputPath).toFile().isFile) {
                    val msg =
                        "If using ${CombineDirectories.COMBINE_PER_INPUT_SUBDIR.optionValue}, all inputs must be directories> $inputPath"
                    throw IllegalArgumentException(msg)
                }
            }
        }
    }

    override fun toString(): String {
        return """    dbPath: ${dbPath}
    inputPaths: ${inputPaths}
    includePathsRegex: ${includePathsRegex}
    excludePathsRegex: ${excludePathsRegex}
    outputPathCsv: ${outputPathCsv}
    queryPerInputSubpart: ${queryPerInputSubpart}
    overwrite: ${overwrite}
    sql: ${sql}
    ignoreLineRegex: ${ignoreLineRegex}
    ignoreFirstLines: ${ignoreFirstLines}
    sortInputPaths: ${sortInputPaths}
    sortInputFileGroups: ${sortInputFileGroups}
    combineInputFiles: ${combineInputFiles}
    combineDirs: ${combineDirs}
    initialRowNumber: ${initialRowNumber}
    jsonExportFormat: ${jsonExportFormat}
    skipNonReadable: ${skipNonReadable}"""
    }

    val mainOutputDir: Path
        get() {
            val outPath: Path = Paths.get(this.outputPathCsv)
            return if (outPath.toFile().isFile) outPath.parent else outPath
        }

    enum class SortInputPaths(override val optionValue: String, private val description: String) : OptionEnum {
        PARAMS_ORDER("paramOrder", "Keep the order from parameters or file system."),
        ALPHA("alpha", "Sort alphabetically."),
        TIME("time", "Sort by modification time, ascending.");

        override val optionName: String by lazy { PARAM_SORT_INPUT_PATHS }

        companion object {
            const val PARAM_SORT_INPUT_PATHS = "sortInputPaths"
            const val PARAM_SORT_FILE_GROUPS = "sortInputFileGroups"
            val optionValues: List<String?>
                get() = EnumUtils.getEnumList(SortInputPaths::class.java).stream()
                    .map { obj: SortInputPaths -> obj.optionValue }
                    .filter { obj: String? -> Objects.nonNull(obj) }
                    .collect(Collectors.toList())
        }

    }

    enum class CombineDirectories(override val optionValue: String) : OptionEnum {
        //USE_EACH_FILE("none"),
        COMBINE_PER_EACH_DIR("perDir"),
        COMBINE_PER_INPUT_DIR("perInputDir"),
        COMBINE_PER_INPUT_SUBDIR("perInputSubdir"),
        COMBINE_ALL_FILES("all");

        override val optionName: String by lazy { PARAM_NAME }

        companion object {
            const val PARAM_NAME = "combineDirs"
            val optionValues: List<String>
                get() = EnumUtils.getEnumList(CombineDirectories::class.java).stream()
                    .map { it.optionValue }
                    .filter { obj: String? -> Objects.nonNull(obj) }
                    .collect(Collectors.toList())
        }
    }

    enum class CombineInputFiles(override val optionValue: String?, private val description: String) : OptionEnum {
        NONE(null, "Uses each input files as a separate table."),
        CONCAT("concat", "Joins the CSV files into one and processes it as input."),
        INTERSECT("intersect", "Takes the intersection of the CSV files as input."),
        EXCEPT("substract", "Substracts 2nd CSV file from the first (only works with 2) and uses it as input.");

        override val optionName: String by lazy { PARAM_NAME }

        companion object {
            const val PARAM_NAME = "combineInputs"
            val optionValues: List<String>
                get() = EnumUtils.getEnumList(CombineInputFiles::class.java).stream()
                    .map(Function<CombineInputFiles, String> { it.optionValue })
                    .filter { obj: String? -> Objects.nonNull(obj) }
                    .collect(Collectors.toList())
        }
    }

    enum class JsonExportFormat(override val optionValue: String?) : OptionEnum {
        NONE(null),
        ENTRY_PER_LINE("entries"),
        ARRAY("array");

        override val optionName: String by lazy { PARAM_NAME }

        companion object {
            const val PARAM_NAME = "json"
        }
    }

    companion object { private val log = logger() }
}
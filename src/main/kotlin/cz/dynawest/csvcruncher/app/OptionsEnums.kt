package cz.dynawest.csvcruncher.app

import org.apache.commons.lang3.EnumUtils
import java.util.*
import java.util.function.Function
import java.util.stream.Collectors

class OptionsEnums {

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

}
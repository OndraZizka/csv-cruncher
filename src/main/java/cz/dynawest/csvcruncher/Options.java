package cz.dynawest.csvcruncher;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

class Options
{
    protected List<String> inputPaths = new ArrayList<>();
    protected String sql;
    protected String outputPathCsv;
    protected String dbPath = null;

    protected int ignoreFirstLines = 1;
    protected Pattern ignoreLineRegex;

    protected Long initialRowNumber = null;
    protected SortInputFiles sortInputFiles = SortInputFiles.PARAMS_ORDER;
    protected CombineInputFiles combineInputFiles = CombineInputFiles.NONE;
    protected CombineDirectories combineDirs = CombineDirectories.COMBINE_PER_EACH_DIR;
    protected JsonExportFormat jsonExportFormat = JsonExportFormat.NONE;


    public boolean isFilled()
    {
        return this.inputPaths != null && this.outputPathCsv != null && this.sql != null;
    }

    public String toString()
    {
        return   "    dbPath: " + this.dbPath +
               "\n    inputPaths: " + this.inputPaths +
               "\n    outputPathCsv: " + this.outputPathCsv +
               "\n    sql: " + this.sql +
               "\n    ignoreLineRegex: " + this.ignoreLineRegex +
               "\n    ignoreFirstLines: " + this.ignoreFirstLines +
               "\n    sortInputFiles: " + this.sortInputFiles +
               "\n    combineInputFiles: " + this.combineInputFiles +
               "\n    combineDirs: " + this.combineDirs +
               "\n    initialRowNumber: " + this.initialRowNumber +
               "\n    jsonExportFormat: " + this.jsonExportFormat;
    }


    public enum SortInputFiles implements OptionEnum {
        PARAMS_ORDER("paramOrder", "Keep the order from parameters."),
        ALPHA("alpha", "Sort alphabetically."),
        TIME("time", "Sort by modification time, ascending.");

        private final String optionValue;
        private final String description;

        SortInputFiles(String value, String description) {
            this.optionValue = value;
            this.description = description;
        }

        @Override
        public String getOptionValue() { return optionValue; }
    }

    public enum CombineDirectories implements OptionEnum
    {
        //USE_EACH_FILE("none"),
        COMBINE_PER_EACH_DIR("perDir"),
        COMBINE_PER_INPUT_DIR("perInputDir"),
        COMBINE_PER_INPUT_SUBDIR("perInputSubdir"),
        COMBINE_ALL_FILES("all");

        public static final String PARAM_NAME = "combineDirs";

        private final String optionValue;

        CombineDirectories(String optionValue) {
            this.optionValue = optionValue;
        }

        public String getOptionValue() { return optionValue; }
    }

    public enum CombineInputFiles implements OptionEnum
    {
        NONE(      null,         "Uses each input files as a separate table."),
        CONCAT(    "concat",     "Joins the CSV files into one and processes it as input."),
        INTERSECT( "intersect",  "Takes the intersection of the CSV files as input."),
        EXCEPT(    "substract",  "Substracts 2nd CSV file from the first (only works with 2) and uses it as input.");

        public static final String PARAM_NAME = "combineInputs";

        private final String description;
        private final String optionValue;

        CombineInputFiles(String value, String description) {
            this.description = description;
            this.optionValue = value;
        }

        public String getOptionValue() { return optionValue; }
    }

    public enum JsonExportFormat implements OptionEnum
    {
        NONE(null), ENTRY_PER_LINE("entries"), ARRAY("array");

        public static final String PARAM_NAME = "json";

        private final String optionValue;

        JsonExportFormat(String value) {
            this.optionValue = value;
        }

        public String getOptionValue() { return optionValue; }
    }
}

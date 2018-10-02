package cz.dynawest.csvcruncher;

import java.util.ArrayList;
import java.util.List;

class Options
{
    protected List<String> inputPaths = new ArrayList<>();
    protected String sql;
    protected String outputPathCsv;
    protected String dbPath = null;
    protected JsonExportFormat jsonExportFormat = JsonExportFormat.NONE;
    protected Long initialRowNumber = null;
    protected SortInputFiles sortInputFiles = SortInputFiles.PARAMS_ORDER;
    protected CombineInputFiles combineInputFiles = CombineInputFiles.NONE;
    protected CombineDirectories combineDirs = CombineDirectories.COMBINE_PER_EACH_DIR;

    public boolean isFilled()
    {
        return this.inputPaths != null && this.outputPathCsv != null && this.sql != null;
    }

    public String toString()
    {
        return "\n    dbPath: " + this.dbPath + "\n    inputPaths: " + this.inputPaths + "\n    outputPathCsv: " + this.outputPathCsv + "\n    sql: " + this.sql;
    }


    public enum SortInputFiles {
        PARAMS_ORDER("paramOrder", "Keep the order from parameters."),
        ALPHA("alpha", "Sort alphabetically."),
        TIME("time", "Sort by modification time, ascending.");

        private final String optionValue;
        private final String description;

        SortInputFiles(String value, String description) {
            this.optionValue = value;
            this.description = description;
        }
    }

    public enum CombineDirectories {
        //USE_EACH_FILE("none"),
        COMBINE_PER_EACH_DIR("perDir"),
        COMBINE_PER_INPUT_DIR("perInputDir"),
        COMBINE_PER_INPUT_SUBDIR("perRootSubdir"),
        COMBINE_ALL_FILES("all");

        private final String combineMode;

        CombineDirectories(String combineMode) {
            this.combineMode = combineMode;
        }
    }

    public enum CombineInputFiles
    {
        NONE("Uses each input files as a separate table."),
        CONCAT("Joins the CSV files into one and processes it as input."),
        INTERSECT ("Takes the intersection of the CSV files as input."),
        EXCEPT("Substracts 2nd CSV file from the first (only works with 2) and uses it as input.");

        private final String description;

        CombineInputFiles(String description) {
            this.description = description;
        }
    }

    public enum JsonExportFormat
    {
        NONE(null), ENTRY_PER_LINE("entries"), ARRAY("array");

        private final String optionsValue;

        JsonExportFormat(String value) {
            this.optionsValue = value;
        }

        public String getOptionsValue() {
            return optionsValue;
        }
    }
}

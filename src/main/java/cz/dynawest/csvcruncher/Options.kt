package cz.dynawest.csvcruncher;

import static cz.dynawest.csvcruncher.Options.CombineDirectories.COMBINE_PER_INPUT_SUBDIR;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;

@Setter
@Getter
@Slf4j
public final class Options
{
    protected List<String> inputPaths = new ArrayList<>();
    protected Pattern includePathsRegex;
    protected Pattern excludePathsRegex;
    protected boolean skipNonReadable = false;
    protected String sql;
    protected String outputPathCsv;

    /**
     * The input files in some file group may have different structure.
     * Normally, that causes a processing error and fails.
     * With this option, it is possible to handle such cases, but the SQL needs to be generic.
     * For each different structure, a table is created and processed separatedly.
     * In this mode, other tables may not be available under the expected names.
     * The SQL may reliably referer only to the processed table as "$table".
     */
    protected boolean queryPerInputSubpart = false;
    protected boolean overwrite = false;
    protected String dbPath = null;
    public boolean keepWorkFiles = false;

    protected int ignoreFirstLines = 1;
    protected Pattern ignoreLineRegex;

    protected Long initialRowNumber = null;
    protected SortInputPaths sortInputPaths = SortInputPaths.PARAMS_ORDER;
    protected SortInputPaths sortInputFileGroups = SortInputPaths.ALPHA;
    protected CombineInputFiles combineInputFiles = CombineInputFiles.NONE;
    protected CombineDirectories combineDirs = CombineDirectories.COMBINE_PER_EACH_DIR;
    protected JsonExportFormat jsonExportFormat = JsonExportFormat.NONE;


    public boolean isFilled()
    {
        return this.inputPaths != null && this.outputPathCsv != null; // && this.sql != null;
    }


    public void validate() throws FileNotFoundException
    {
        if (this.inputPaths == null || this.inputPaths.isEmpty())
            throw new IllegalArgumentException(" -in is not set.");

        // SQL may be omitted if there is a request to combine files or convert to JSON. Otherwise it would be a no-op.
        if (this.sql == null) {
            log.debug(" -sql is not set, using default: " + Cruncher.DEFAULT_SQL);
            this.sql = Cruncher.DEFAULT_SQL;
        }

        if (this.outputPathCsv == null)
            throw new IllegalArgumentException(" -out is not set.");


        for (String path : this.inputPaths) {
            File ex = new File(path);
            if (!ex.exists())
                throw new FileNotFoundException("CSV file not found: " + ex.getPath());
        }

        if (this.queryPerInputSubpart && !StringUtils.isBlank(this.sql) && !this.sql.contains(Cruncher.SQL_TABLE_PLACEHOLDER)) {
            String msg = String.format("queryPerInputSubpart is enabled, but the SQL is not generic (does not use %s), which doesn't make sense.", Cruncher.SQL_TABLE_PLACEHOLDER);
            throw new IllegalArgumentException(msg);
        }

        if (COMBINE_PER_INPUT_SUBDIR.equals(this.combineDirs)) {
            for (String inputPath : this.inputPaths) {
                if (Paths.get(inputPath).toFile().isFile()) {
                    String msg = String.format("If using %s, all inputs must be directories> %s", COMBINE_PER_INPUT_SUBDIR.getOptionValue(), inputPath);
                    throw new IllegalArgumentException(msg);
                }
            }

        }
    }



    public String toString()
    {
        return   "    dbPath: " + this.dbPath +
               "\n    inputPaths: " + this.inputPaths +
               "\n    includePathsRegex: " + this.includePathsRegex +
               "\n    excludePathsRegex: " + this.excludePathsRegex +
               "\n    outputPathCsv: " + this.outputPathCsv +
               "\n    queryPerInputSubpart: " + this.queryPerInputSubpart +
               "\n    overwrite: " + this.overwrite +
               "\n    sql: " + this.sql +
               "\n    ignoreLineRegex: " + this.ignoreLineRegex +
               "\n    ignoreFirstLines: " + this.ignoreFirstLines +
               "\n    sortInputPaths: " + this.sortInputPaths +
               "\n    sortInputFileGroups: " + this.sortInputFileGroups +
               "\n    combineInputFiles: " + this.combineInputFiles +
               "\n    combineDirs: " + this.combineDirs +
               "\n    initialRowNumber: " + this.initialRowNumber +
               "\n    jsonExportFormat: " + this.jsonExportFormat +
               "\n    skipNonReadable: " + this.skipNonReadable;
    }

    public Path getMainOutputDir()
    {
        Path outPath = Paths.get(this.getOutputPathCsv());

        if (outPath.toFile().isFile())
            return outPath.getParent();
        else
            return outPath;
    }


    public enum SortInputPaths implements OptionEnum
    {
        PARAMS_ORDER("paramOrder", "Keep the order from parameters or file system."),
        ALPHA("alpha", "Sort alphabetically."),
        TIME("time", "Sort by modification time, ascending.");

        public static final String PARAM_SORT_INPUT_PATHS = "sortInputPaths";
        public static final String PARAM_SORT_FILE_GROUPS = "sortInputFileGroups";

        private final String optionValue;
        private final String description;

        SortInputPaths(String value, String description) {
            this.optionValue = value;
            this.description = description;
        }

        @Override
        public String getOptionValue() { return optionValue; }

        public static List<String> getOptionValues() {
            return EnumUtils.getEnumList(SortInputPaths.class).stream().map(SortInputPaths::getOptionValue).filter(Objects::nonNull).collect(Collectors.toList());
        }
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

        public static List<String> getOptionValues()
        {
            return EnumUtils.getEnumList(Options.CombineDirectories.class).stream().map(Options.CombineDirectories::getOptionValue).filter(Objects::nonNull).collect(Collectors.toList());
        }
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

        public static List<String> getOptionValues()
        {
            return EnumUtils.getEnumList(Options.CombineInputFiles.class).stream().map(Options.CombineInputFiles::getOptionValue).filter(Objects::nonNull).collect(Collectors.toList());
        }
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

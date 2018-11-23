package cz.dynawest.csvcruncher.util;

import cz.dynawest.csvcruncher.Cruncher;
import cz.dynawest.csvcruncher.CruncherInputSubpart;
import cz.dynawest.csvcruncher.CsvCruncherException;
import cz.dynawest.csvcruncher.Options;
import static cz.dynawest.csvcruncher.Options.SortInputPaths.PARAMS_ORDER;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.event.Level;


/**
 * TODO: Convert the concat related methods to a context-based class.
 */
@Slf4j
public class FilesUtils
{
    private static final String CONCAT_WORK_SUBDIR_NAME = "concat";

    /**
     * Concatenates given files into a file in the resultPath, named "CsvCruncherConcat.csv".
     * If some of the input files does not end with a new line, it is appended after that file.
     * @return The path to the created file.
     */
    static Path concatFiles(List<Path> filesToConcat, final Path resultPath, final int ignoreFirstLines, final Pattern ignoreLineRegex)
    {
        File resultFile = resultPath.toFile();
        Matcher ignoreLineMatcher = ignoreLineRegex == null ? null : ignoreLineRegex.matcher("");
        boolean headerIncluded = false;

        //  try(FileOutputStream resultOS = new FileOutputStream(resultFile);) {
        try(FileWriter resultWriter = new FileWriter(resultFile)) {
            for (Path pathToConcat : filesToConcat) {
                //try (FileInputStream fileToConcatIS = new FileInputStream(pathToConcat.toFile())) {
                //    IOUtils.copy(fileToConcatReader, resultOS);
                int linesCountDown = ignoreFirstLines;
                try (BufferedReader fileToConcatReader = new BufferedReader(new InputStreamReader(new FileInputStream(pathToConcat.toFile())))) {
                    String line;
                    while (null != (line = fileToConcatReader.readLine()))
                    {
                        linesCountDown--;
                        ///System.out.printf("LINE: h: %b lcd: %d LINE:  %s\n", headerIncluded, linesCountDown, line); //
                        if (headerIncluded && linesCountDown >= 0)
                            continue;
                        if (headerIncluded && null != ignoreLineMatcher && ignoreLineMatcher.reset(line).matches())
                            continue;
                        headerIncluded |= true; // Take the very first line.
                        ///System.out.println("MADE IT...");

                        resultWriter.append(line).append("\n");
                    }
                }

                // Read the last byte, check if it's a \n. If not, let's append one.
                /*try (FileInputStream fileToConcatIS = new FileInputStream(pathToConcat.toFile())) {
                    fileToConcatIS.skip(pathToConcat.toFile().length()-1);
                    if ('\n' != fileToConcatIS.read())
                        resultWriter.write('\n');
                }*/
            }
        }
        catch (Exception ex) {
            throw new CsvCruncherException("Failed concatenating files into " + resultPath + ": " + ex.getMessage(), ex);
        }
        return  resultFile.toPath();
    }

    public static List<Path> sortInputPaths(List<Path> inputPaths, Options.SortInputPaths sortMethod)
    {
        switch (sortMethod) {
            case PARAMS_ORDER:
                return Collections.unmodifiableList(inputPaths);

            case ALPHA:
                inputPaths = new ArrayList<>(inputPaths);
                Collections.sort(inputPaths);
                return inputPaths;

            case TIME: throw new UnsupportedOperationException("Sorting by time not implemented yet.");
            default: throw new UnsupportedOperationException("Unkown sorting method.");
        }
    }

    /**
     * Writes the given resultset to a JSON file at given path, one entry per line, optionally as an JSON array.
     */
    public static void convertResultToJson(ResultSet resultSet, Path destFile, boolean printAsArray)
    {
        try (
                OutputStream outS = new BufferedOutputStream(new FileOutputStream(destFile.toFile()));
                Writer outW = new OutputStreamWriter(outS, StandardCharsets.UTF_8)
        ) {
            ResultSetMetaData metaData = resultSet.getMetaData();

            // Cache which cols are numbers.

            //boolean[] colsAreNumbers = cacheWhichColumnsNeedJsonQuotes(metaData);


            if (printAsArray)
                outW.append("[\n");

            while (resultSet.next()) {
                // javax.json way
                JsonObjectBuilder builder = Json.createObjectBuilder();
                // Columns
                for (int colIndex = 1; colIndex <= metaData.getColumnCount(); colIndex++) {
                    addTheRightTypeToJavaxJsonBuilder(resultSet, colIndex, builder);
                }
                JsonObject jsonObject = builder.build();
                JsonWriter writer = Json.createWriter(outW);
                writer.writeObject(jsonObject);


                /*// Hand-made
                outW.append("{\"");
                // Columns
                for (int colIndex = 1; colIndex <= metaData.getColumnCount(); colIndex++) {
                    // Key
                    outW.append(org.json.simple.JSONObject.escape(metaData.getColumnLabel(colIndex) ));
                    outW.append("\":");
                    // TODO
                    String val = formatValueForJson(resultSet, colIndex, colsAreNumbers);
                    if (null == val) {
                        outW.append("null");
                        continue;
                    }
                    if (!colsAreNumbers[colIndex])
                        outW.append('"');
                    outW.append(val);
                    if (!colsAreNumbers[colIndex])
                        outW.append('"');
                }
                outW.append("\"}");
                /**/

                outW.append(printAsArray ? ",\n" : "\n");
            }
            if (printAsArray)
                outW.append("]\n");
        }
        catch (Exception ex) {
            throw new CsvCruncherException("Failed browsing the final query results: " + ex.getMessage(), ex);
        }
    }


    private static boolean[] cacheWhichColumnsNeedJsonQuotes(ResultSetMetaData metaData) throws SQLException
    {
        boolean[] colsAreNumbers = new boolean[metaData.getColumnCount()+1];
        int colType;
        for (int colIndex = 1; colIndex <= metaData.getColumnCount(); colIndex++ ) {
            colType = metaData.getColumnType(colIndex);
            colsAreNumbers[colIndex] =
                colType == Types.TINYINT || colType == Types.SMALLINT || colType == Types.INTEGER || colType == Types.BIGINT
                || colType == Types.DECIMAL || colType == Types.NUMERIC ||  colType == Types.BIT || colType == Types.ROWID || colType == Types.DOUBLE || colType == Types.FLOAT || colType == Types.BOOLEAN;
        }
        return colsAreNumbers;
    }

    /**
     * Used in case we use javax.json.JsonBuilder.
     * This also needs JsonProviderImpl.
     */
    private static void addTheRightTypeToJavaxJsonBuilder(ResultSet resultSet, int colIndex, JsonObjectBuilder builder) throws SQLException
    {
        if (resultSet.getObject(colIndex) == null)
            return;

        ResultSetMetaData metaData = resultSet.getMetaData();
        String columnLabel = metaData.getColumnLabel(colIndex);
        if (columnLabel.matches("[A-Z]+"))
            columnLabel = columnLabel.toLowerCase();

        switch (metaData.getColumnType(colIndex)) {
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.CLOB:
                builder.add(columnLabel, resultSet.getString(colIndex)); break;
            case Types.TINYINT:
            case Types.BIT:
                builder.add(columnLabel, resultSet.getByte(colIndex)); break;
            case Types.SMALLINT: builder.add(columnLabel, resultSet.getShort(colIndex)); break;
            case Types.INTEGER:  builder.add(columnLabel, resultSet.getInt(colIndex)); break;
            case Types.BIGINT:   builder.add(columnLabel, resultSet.getLong(colIndex)); break;
            case Types.BOOLEAN:  builder.add(columnLabel, resultSet.getBoolean(colIndex)); break;
            case Types.FLOAT:
            case Types.DOUBLE:   builder.add(columnLabel, resultSet.getDouble(colIndex)); break; // Same for HSQLDB
            case Types.DECIMAL:
            case Types.NUMERIC:  builder.add(columnLabel, resultSet.getBigDecimal(colIndex)); break;
            case Types.DATE:    builder.add(columnLabel, ""+resultSet.getDate(colIndex)); break;
            case Types.TIME:    builder.add(columnLabel, ""+resultSet.getTime(colIndex)); break;
            case Types.TIMESTAMP:    builder.add(columnLabel, (""+resultSet.getTimestamp(colIndex)).replace(' ', 'T')); break; // JS Date() takes "1995-12-17T03:24:00"
        }
    }

    /**
     * Expands the input paths if they are directories, sorts the resulting groups, filters by the options includes/excludes.
     *
     * @return Input files grouped by the given input paths. CSV headers are not yet checked.
     */
    public static Map<Path, List<Path>> expandFilterSortInputFilesGroups(List<Path> inputPaths, Options options)
    {
        if (Options.CombineInputFiles.NONE.equals(options.getCombineInputFiles()) )
        {
            // No splitting - return a list with the same item.
            return mapOfIdentityToSingletonList(inputPaths);
        }

        // Expand the directories.
        Map<Path, List<Path>> fileGroupsToCombine = expandDirectories(inputPaths, options);

        // Filter
        fileGroupsToCombine = filterFileGroups(options, fileGroupsToCombine);
        logFileGroups(fileGroupsToCombine, Level.DEBUG, "Filtered file groups:");

        // If there is just one catch-all group...
        if (fileGroupsToCombine.size() == 1 && fileGroupsToCombine.keySet().iterator().next() == null) {
            List<Path> paths = fileGroupsToCombine.get(null);
            if (paths.isEmpty()) {
                log.info("   *** No files found.");
                return Collections.emptyMap();
            }

            if (paths.size() == 1) {
                return mapOfIdentityToSingletonList(paths);
            }

            // If there is only one input path, use it as the originating path. Maybe it should be done in combine()?
            if (inputPaths.size() == 1) {
                fileGroupsToCombine.remove(null);
                fileGroupsToCombine.put(inputPaths.get(0), paths);
            }
        }

        fileGroupsToCombine = sortFileGroups(options, fileGroupsToCombine);
        logFileGroups(fileGroupsToCombine, Level.DEBUG, "Sorted and filtered file groups:");

        return fileGroupsToCombine;
    }

    /**
     * Combine the input files (typically, concatenate).
     * If the paths are directories, they may be combined per each directory, per input dir, per input subdir, or all into one.
     * The combined input files will be witten under the respective "group root directory".
     * For COMBINE_ALL_FILES, the combined file will be written under current user directory ("user.dir").
     *
     * @return Mapping from the concatenated file to the files that ended up in it.
     */
    public static List<CruncherInputSubpart> combineInputFiles(Map<Path, List<Path>> fileGroupsToCombine, Options options) throws IOException
    {
        // Split into subgroups by column names in the CSV header.
        FileGroupsSplitBySchemaResult splitResult = splitToSubgroupsPerSameHeaders(fileGroupsToCombine);
        fileGroupsToCombine = splitResult.getFileGroupsToCombine();
        logFileGroups(fileGroupsToCombine, Level.DEBUG, "File groups split per header structure:");

        // At this point, the group keys are the original group + _<counter>.
        // TODO: Again, refactor this to something more sane.

        // Get the final concatenated file path. Currently, "-out" + _concat.
        Path destDir = Paths.get(options.getMainOutputDir().toString() + "_" + CONCAT_WORK_SUBDIR_NAME);

        switch (options.getCombineInputFiles()) {
            case INTERSECT:
            case EXCEPT:
                throw new UnsupportedOperationException("INTERSECT and EXCEPT combining is not implemented yet.");

            case CONCAT:
                log.debug("Concatenating input files.");
                return concatenateFilesFromFileGroups(options, fileGroupsToCombine, destDir);
        }

        throw new IllegalStateException("Did we miss some CombineInputFiles choice?");
    }


    private static Map<Path, List<Path>> mapOfIdentityToSingletonList(List<Path> inputPaths)
    {
        return inputPaths.stream().collect(Collectors.toMap(x -> x, Collections::singletonList));
    }


    private static void logFileGroups(Map<Path, List<Path>> fileGroupsToConcat, org.slf4j.event.Level level, String label)
    {
        // TBD: Apply level.
        /*SubstituteLoggingEvent event = new SubstituteLoggingEvent();
        event.setLevel(level);
        event.setMessage();
        ((Logger) log).log(event);*/

        log.debug("--- " + label + " ---" );
        for (Map.Entry<Path, List<Path>> fileGroup : fileGroupsToConcat.entrySet()) {
            String msg = "\n * Path: " + fileGroup.getKey() + ": "
                    + fileGroup.getValue().stream().map(path -> "\n\t- " + path).collect(Collectors.joining());
            log.debug(msg);
        }
    }


    /**
     * Walks through the directories given in inputPaths and expands them into the contained files,
     * into groups as per rules given by options - see {@link Options.CombineDirectories}, {@link Options#skipNonReadable}.
     *
     * @return A map with one entry per group, containing the files.
     */
    static Map<Path, List<Path>> expandDirectories(List<Path> inputPaths, Options options)
    {
        Map<Path, List<Path>> fileGroupsToConcat = new HashMap<>();
        // null will be used as a special key for COMBINE_ALL_FILES.
        fileGroupsToConcat.put(null, new ArrayList<>());

        for (Path inputPath: inputPaths) try {
            log.debug(" * About to concat " + inputPath);

            // Put files simply to "global" group. Might be improved in the future.
            if (!inputPath.toFile().exists())
                throw new IllegalStateException("File does not exist: " + inputPath);

            // Put files simply to "global" group. Might be improved in the future.
            if (inputPath.toFile().isFile())
                fileGroupsToConcat.get(null).add(inputPath);
            // Walk directories for CSV, and group them as per options.combineDirs.
            if (inputPath.toFile().isDirectory())
            {
                Consumer<Path> fileToGroupSorter = null;
                switch (options.getCombineDirs()) {
                    case COMBINE_ALL_FILES: {
                        List<Path> fileGroup = fileGroupsToConcat.get(null);
                        fileToGroupSorter = fileGroup::add;
                    } break;
                    case COMBINE_PER_INPUT_DIR: {
                        List<Path> fileGroup = fileGroupsToConcat.get(inputPath);
                        fileToGroupSorter = fileGroup::add;
                    } break;
                    case COMBINE_PER_EACH_DIR: {
                        //List<Path> fileGroup = fileGroupsToCombine.get(inputPath);
                        fileToGroupSorter = curFile -> {
                            fileGroupsToConcat.computeIfAbsent(curFile.toAbsolutePath().getParent(),  (Path k) -> new ArrayList<>()).add(curFile);
                        };
                    } break;
                    case COMBINE_PER_INPUT_SUBDIR: {
                        throw new UnsupportedOperationException("Not yet implemented"); // TODO
                    }
                }

                log.trace("   *** About to walk" + inputPath);
                Files.walk(inputPath)
                        .filter(curFile -> Files.isRegularFile(curFile) && curFile.getFileName().toString().endsWith(Cruncher.FILENAME_SUFFIX_CSV))
                        ///.peek(path -> System.out.println("fileToGroupSorter " + path))
                        .filter(file -> {
                            if (file.toFile().canRead()) return true;
                            if (options.isSkipNonReadable()) {
                                log.info("Skipping non-readable file: " + file);
                                return false;
                            }
                            throw new IllegalArgumentException("Unreadable file (try --skipNonReadable): " + file);
                        } )
                        .forEach(fileToGroupSorter);
                log.trace("   *** After walking: " + fileGroupsToConcat);
            }
        } catch (Exception ex) {
            throw new CsvCruncherException(String.format("Failed combining the input files in %s: %s", inputPath, ex.getMessage()), ex);
        }
        return fileGroupsToConcat;
    }

    /**
     * Reduces the groups to only contain files that match the include and don't match the exclude pattern - see {@link Options#includePathsRegex}.
     * Also, skips the empty groups.
     */
    static Map<Path, List<Path>> filterFileGroups(Options options, Map<Path, List<Path>> fileGroupsToConcat)
    {
        Map<Path, List<Path>> fileGroupsToConcat2 = new HashMap<>();

        for (Map.Entry<Path, List<Path>> fileGroup : fileGroupsToConcat.entrySet()) {
            List<Path> filteredPaths = filterPaths(options, fileGroup.getValue());

            Path origin = fileGroup.getKey();
            List<Path> paths = fileGroup.getValue();
            if (paths.isEmpty()) {
                if (origin != null)
                    log.info("   *** No files found in " + origin + ".");
                continue;
            }

            fileGroupsToConcat2.put(origin, filteredPaths);
        }
        return fileGroupsToConcat2;
    }

    public static List<Path> filterPaths(Options options, List<Path> paths)
    {
        if (options.getIncludePathsRegex() == null && options.getExcludePathsRegex() == null)
            return paths;

        return paths.stream()
                .filter(path -> options.getIncludePathsRegex() == null || options.getIncludePathsRegex().matcher(path.toString()).matches())
                .filter(path -> options.getExcludePathsRegex() == null || !options.getExcludePathsRegex().matcher(path.toString()).matches())
                .collect(Collectors.toList());
    }

    /**
     * Sorts the files within the groups by the configured sorting - see {@link Options.SortInputPaths}. Skips the empty groups.
     * @return A map with one entry per group, containing the files in sorted order.
     */
    private static Map<Path, List<Path>> sortFileGroups(Options options, Map<Path, List<Path>> fileGroupsToConcat)
    {
        if (PARAMS_ORDER.equals(options.getSortInputFileGroups()))
            throw new IllegalStateException("Input file groups have to be sorted somehow, " + PARAMS_ORDER.getOptionValue() + " not applicable.");

        Map<Path, List<Path>> fileGroupsToConcat2 = new HashMap<>();

        for (Map.Entry<Path, List<Path>> fileGroup : fileGroupsToConcat.entrySet()) {
            Path origin = fileGroup.getKey();
            List<Path> sortedPaths = sortInputPaths(fileGroup.getValue(), options.getSortInputFileGroups());
            fileGroupsToConcat2.put(origin, sortedPaths);

            String dirLabel = origin == null ? "all files" : "" + origin;
            log.debug("   *** Will combine files from " + dirLabel + ": "
                    + sortedPaths.stream().map(path -> "\n\t* " + path).collect(Collectors.joining()));
        }
        return fileGroupsToConcat2;
    }


    /**
     * Some groups may contain CSV files which have different headers;
     * this splits such groups into subgroups and puts them separatedly to returned fileGroups.
     * Takes the order into account - a group is created each time the headers change, even if they change to previously seen structure.
     * The new subgroups names are the <original group path> + _X, where X is an incrementing number.
     *
     * @return A map with one entry per group, containing the files in the original order, but split per CSV header structure.
     */
    private static FileGroupsSplitBySchemaResult splitToSubgroupsPerSameHeaders(Map<Path, List<Path>> fileGroupsToConcat) throws IOException
    {
        // TODO: Record information about what groups were splitted and to what subgroups.
        Map<Path, List<Path>> fileGroupsToConcat2 = new LinkedHashMap<>();
        Map<Path, List<Path>> splittedGroupsInfo_oldGroupToNewGroups = new LinkedHashMap<>();
        FileGroupsSplitBySchemaResult result = new FileGroupsSplitBySchemaResult(fileGroupsToConcat2, splittedGroupsInfo_oldGroupToNewGroups);
        // TODO: Refactor this into proper models. InputGroup, PerHeaderInputSubgroup, etc.

        for (Map.Entry<Path, List<Path>> fileGroup : fileGroupsToConcat.entrySet()) {
            // Check if all files have the same columns header.
            Map<List<String>, List<Path>> subGroups_headerStructureToFiles = new HashMap<>();
            for (Path fileToConcat : fileGroup.getValue()) {
                List<String> headers = parseColsFromFirstCsvLine(fileToConcat.toFile());
                subGroups_headerStructureToFiles.computeIfAbsent(headers, x -> new ArrayList<>()).add(fileToConcat);
            }

            Path originalGroupPath = fileGroup.getKey();
            if (subGroups_headerStructureToFiles.size() == 1) {
                fileGroupsToConcat2.put(originalGroupPath, fileGroup.getValue());
            }
            else {
                // Replaces the original group with few subgroups, with paths suffixed with counter: originalPath_1, originalPath_2, ...
                int counter = 1;
                for (List<Path> filesWithSameHeaders : subGroups_headerStructureToFiles.values())
                {
                    Path subgroupKey = Paths.get("" + originalGroupPath + "_" + counter++);
                    fileGroupsToConcat2.putIfAbsent(subgroupKey, filesWithSameHeaders);
                    // Keep information about what groups were splitted and to what subgroups.
                    splittedGroupsInfo_oldGroupToNewGroups
                            .computeIfAbsent(originalGroupPath, x -> new ArrayList<>())
                            .add(subgroupKey);
                }
            }
        }
        return result;
    }

    /**
     * @param options Used to filter the files, like "skip 1st line" or regex line matches.
     * @param fileGroupsToConcat Mapping from file group "key" (original group path + counter suffix) to the list of files to be concatenated.
     * @param tmpConcatDir  A directory where the concatenation results shoul go.
     * @return Mapping from the resulting concatenated file to the files that were concatenated.
     */
    private static List<CruncherInputSubpart> concatenateFilesFromFileGroups(Options options, Map<Path, List<Path>> fileGroupsToConcat, Path tmpConcatDir) throws IOException
    {
        List<CruncherInputSubpart> inputSubparts = new ArrayList<>();
        Set<Path> usedConcatFilePaths = new HashSet<>();

        for (Map.Entry<Path, List<Path>> fileGroup : fileGroupsToConcat.entrySet()) {
            // Destination directory
            //log.debug("    Into dest dir: " + tmpConcatDir);
            Files.createDirectories(tmpConcatDir);

            String concatFileName = deriveNameForCombinedFile(fileGroup, usedConcatFilePaths);
            Path concatenatedFilePath = tmpConcatDir.resolve(concatFileName);
            usedConcatFilePaths.add(concatenatedFilePath);
            log.debug("    Into dest file: " + concatenatedFilePath + "\n  will combine these files: "
                    + fileGroup.getValue().stream().map(path -> "\n\t* " + path).collect(Collectors.joining()));

            // TODO: Optionally this could be named better:
            //       1) Find common deepest ancestor dir.
            //       2) From each dest path, substract the differentiating subpath.
            //       3) Create the subdirs in tmpConcatDir and save there.

            // Combine the file sets.
            concatFiles(fileGroup.getValue(), concatenatedFilePath, options.getIgnoreFirstLines(), options.getIgnoreLineRegex());

            CruncherInputSubpart inputPart = new CruncherInputSubpart();
            inputPart.setOriginalInputPath(fileGroup.getKey());
            inputPart.setCombinedFile(concatenatedFilePath);
            inputPart.setCombinedFromFiles(fileGroup.getValue());

            inputSubparts.add(inputPart);
        }
        return inputSubparts;
    }



    /**
     * Come up with some good name for the combined file.
     * If the name was used, append an incrementing number until it is unique.
     *
     * @param fileGroup  A group of files to combine in the value, and the originating input path in the value.
     */
    static String deriveNameForCombinedFile(Map.Entry<Path, List<Path>> fileGroup, Set<Path> usedConcatFilePaths)
    {
        Path originPath = fileGroup.getKey();
        if (originPath == null) {
            // Assorted files will be combined into resultDir/concat.csv.
            return Paths.get("concat" + Cruncher.FILENAME_SUFFIX_CSV).toString();
        }
        else {
            String pathStr = StringUtils.appendIfMissing(originPath.toString(), Cruncher.FILENAME_SUFFIX_CSV);
            originPath = Paths.get(pathStr);

            Path concatFilePath = getNonUsedName(originPath, usedConcatFilePaths);
            usedConcatFilePaths.add(concatFilePath);
            return concatFilePath.getFileName().toString();
        }
    }

    /**
     * Returns a path derived from path, that is not already used (present in the usedPaths set).
     * The paths derived have the file base name + "_{counter}".
     * Eg. given "some/output/file.csv", if already used, tries "some/output/file_1.csv", and so on.
     * It does NOT add the used path to the usedPaths set.
     */
    public static Path getNonUsedName(Path path, final Set<Path> usedPaths)
    {
        if (!usedPaths.contains(path))
            return path;


        String suffix = StringUtils.substringAfterLast(""+path.getFileName(), ".");
        if (!StringUtils.isEmpty(suffix))
            suffix = "." + suffix;

        String pathWithoutSuffix = StringUtils.removeEnd(path.toString(), suffix);

        int counter = 0;
        do {
            path = Paths.get(pathWithoutSuffix + "_" + ++counter + suffix);
        }
        while (usedPaths.contains(path));
        usedPaths.add(path);

        return path;
    }


    /**
     * Parse the first lien of given file, ignoring the initial #'s, NOT respecting quotes and escaping.
     *
     * @return A list of column names in the order from the file.
     */
    public static List<String> parseColsFromFirstCsvLine(File file) throws IOException
    {
        Matcher mat = Cruncher.REGEX_SQL_COLUMN_VALID_NAME.matcher("");
        ArrayList<String> cols = new ArrayList<>();

        LineIterator lineIterator = FileUtils.lineIterator(file);
        if (!lineIterator.hasNext())
        {
            throw new IllegalStateException("No first line with columns definition (format: [# ] <colName> [, ...]) in: " + file.getPath());
        }
        else
        {
            /*  I could employ CSVReader if needed.
                CSVReader csvReader = new CSVReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                String[] header = csvReader.readNext();
                csvReader.close();
             */

            String line = lineIterator.nextLine().trim();
            line = StringUtils.stripStart(line, "#");
            String[] colNames = StringUtils.splitPreserveAllTokens(line, ",;");

            for (String colName : Arrays.asList(colNames))
            {
                colName = colName.trim();
                if (colName.isEmpty()) {
                    throw new IllegalStateException(String.format(
                            "Empty column name (separators: ,; ) in: %s\n  The line was: %s",
                            file.getPath(), line
                    ));
                }

                if (!mat.reset(colName).matches()) {
                    throw new IllegalStateException(String.format(
                            "Colname '%s' must be valid SQL identifier, i.e. must match /%s/i in: %s",
                            colName, Cruncher.REGEX_SQL_COLUMN_VALID_NAME.pattern(), file.getPath()
                    ));
                }

                cols.add(colName);
            }

            return cols;
        }
    }

    /**
     * Checks whether the paths point to existing files.
     */
    public static void validateInputFiles(List<CruncherInputSubpart> inputSubparts)
    {
        List<Path> inputPaths = new ArrayList<>(inputSubparts.stream().map(x -> x.getCombinedFile()).collect(Collectors.toList()));

        List<Path> notFiles = inputPaths.stream().filter(path -> !path.toFile().isFile()).collect(Collectors.toList());
        if (!notFiles.isEmpty()) {
            String msg = "Some input paths do not point to files: " + notFiles.stream().map(Path::toString).collect(Collectors.joining(", "));
            throw new IllegalStateException(msg);
        }
    }

    /**
     * Result of a split into subgroups.
     * When combining CSV files, if they do not match in structure (which is derived from the header names),
     * they are split into subgroups.
     *
     * TODO: Refactor. Previously this was done into a flat structure, but it seems at least 2 levels will be needed.
     *       For now I am only putting it to two flat maps "joined" by the subgroup names.
     */
    @Getter
    @AllArgsConstructor
    static class FileGroupsSplitBySchemaResult
    {
        private Map<Path, List<Path>> fileGroupsToCombine = new LinkedHashMap<>();

        /**
         * Old group to new subgroups.
         */
        private Map<Path, List<Path>> splittedGroupsInfo = new LinkedHashMap<>();
    }

}

package cz.dynawest.csvcruncher;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;
import org.apache.commons.io.IOUtils;

public class FilesUtils
{
    private static final Logger LOG = Logger.getLogger(FilesUtils.class.getName());

    /**
     * Concatenates given files into a file in the resultPath, named "CsvCruncherConcat.csv".
     * If some of the input files does not end with a new line, it is appended after that file.
     * @return The path to the created file.
     */
    static Path concatFiles(List<Path> filesToConcat, Path resultPath)
    {
        File resultFile = resultPath.toFile();
        try(FileOutputStream resultOS = new FileOutputStream(resultFile);) {
            for (Path pathToConcat : filesToConcat) {
                try (FileInputStream fileToConcatIS = new FileInputStream(pathToConcat.toFile())) {
                    IOUtils.copy(fileToConcatIS, resultOS);
                }
                // Read the last byte, check if it's a \n. If not, let's append one.
                try (FileInputStream fileToConcatIS = new FileInputStream(pathToConcat.toFile())) {
                    fileToConcatIS.skip(pathToConcat.toFile().length()-1);
                    if ('\n' != fileToConcatIS.read())
                        resultOS.write('\n');
                }
            }
        }
        catch (Exception ex) {
            throw new RuntimeException("Failed concatenating files into " + resultPath + ": " + ex.getMessage(), ex);
        }
        return  resultFile.toPath();
    }

    static List<Path> sortInputPaths(List<Path> inputPaths, Options.SortInputFiles sortMethod)
    {
        switch (sortMethod) {
            case PARAMS_ORDER: return inputPaths;
            case ALPHA: inputPaths = new ArrayList<>(inputPaths); Collections.sort(inputPaths); return inputPaths;
            case TIME: throw new UnsupportedOperationException("Sorting by time not implemented yet.");
            default: throw new UnsupportedOperationException("Unkown sorting method.");
        }
    }

    /**
     * Writes the given resultset to a JSON file at given path, one entry per line, optionally as an JSON array.
     */
    static void convertResultToJson(ResultSet resultSet, Path destFile, boolean printAsArray)
    {
        try (
                OutputStream outS = new BufferedOutputStream(new FileOutputStream(destFile.toFile()));
                Writer outW = new OutputStreamWriter(outS, StandardCharsets.UTF_8);
        ) {
            ResultSetMetaData metaData = resultSet.getMetaData();

            // Cache which cols are numbers.

            boolean[] colsAreNumbers = cacheWhichColumnsNeedJsonQuotes(metaData);


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
            throw new RuntimeException("Failed browsing the final query results: " + ex.getMessage(), ex);
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
     * Combine the input files (typically, concatenate).
     * If the paths are directories, they may be combined per each directory, per input dir, per input subdir, or all into one.
     * The combined input files will be witten under the respective "group root directory".
     * For COMBINE_ALL_FILES, the combined file will be written under current user directory ("user.dir").
     */
    static List<Path> combineInputFiles(List<Path> inputPaths, Options options) throws IOException
    {
        switch (options.combineInputFiles) {
            case NONE: default: return inputPaths;
            case INTERSECT: case EXCEPT: throw new UnsupportedOperationException("INTERSECT and EXCEPT combining is not implemented yet.");
            case CONCAT:
                LOG.fine("Concatenating input files:");

                // First, expand the directories.
                Map<Path, List<Path>> fileGroupsToConcat = new HashMap<>();
                // null will be used as a special key for COMBINE_ALL_FILES.
                fileGroupsToConcat.putIfAbsent(null, new ArrayList<>());

                List<Path> concatenatedFiles = new ArrayList<>();
                for (Path inputPath: inputPaths) try {
                    LOG.info(" * About to concat " + inputPath);

                    // Put files simply to "global" group. Might be improved in the future.
                    if (inputPath.toFile().isFile())
                        fileGroupsToConcat.get(null).add(inputPath);
                    // Walk directories for CSV, and group them as per options.combineDirs.
                    if (inputPath.toFile().isDirectory()) {

                        Consumer<Path> fileToGroupSorter = null;
                        switch (options.combineDirs) {
                            case COMBINE_ALL_FILES: {
                                List<Path> fileGroup = fileGroupsToConcat.get(null);
                                fileToGroupSorter = curPath -> { fileGroup.add(curPath); };
                                } break;
                            case COMBINE_PER_INPUT_DIR: {
                                List<Path> fileGroup = fileGroupsToConcat.get(inputPath);
                                fileToGroupSorter = curPath -> { fileGroup.add(curPath); };
                                } break;
                            case COMBINE_PER_EACH_DIR: {
                                //List<Path> fileGroup = fileGroupsToConcat.get(inputPath);
                                fileToGroupSorter = curPath -> { fileGroupsToConcat.get(curPath.toAbsolutePath()).add(curPath); };
                            }
                        }

                        LOG.finer("   *** About to walk" + inputPath);
                        Files.walk(inputPath)
                                .filter(curPath -> Files.isRegularFile(curPath) && curPath.getFileName().toString().endsWith(Cruncher.FILENAME_SUFFIX_CSV))
                                .forEach(fileToGroupSorter);
                        LOG.finer("   *** After walking: " + fileGroupsToConcat.size());
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(String.format("Failed combining the input files in %s: %s", inputPath, ex.getMessage()), ex);
                }

                // Then combine the file sets.

                Path defaultDestDir = Paths.get(options.outputPathCsv).getParent(); // Paths.get(System.getProperty("user.dir"));
                int filesCounter = 0;

                for (Map.Entry<Path, List<Path>> fileGroup : fileGroupsToConcat.entrySet())
                {
                    String dirLabel = fileGroup.getKey() == null ? "all files" : ""+fileGroup.getKey();
                    LOG.info("   *** Combining " + dirLabel + ": " + fileGroup.getValue());

                    // Sort
                    List<Path> sortedPaths = sortInputPaths(fileGroup.getValue(), options.sortInputFiles).stream().collect(Collectors.toList());

                    Path destDir = fileGroup.getKey();
                    fileGroupsToConcat.put(destDir, sortedPaths);

                    // Assorted files will be put to the "current dir". Or to the result dir?
                    if (destDir == null) {
                        destDir = defaultDestDir.resolve("concat");
                        Files.createDirectories(destDir);
                    }

                    // Come up with some good name for the combined file.
                    Path concatenatedFilePath =
                        destDir.resolve(destDir.getFileName().toString()
                            + "_" + ++filesCounter + Cruncher.FILENAME_SUFFIX_CSV);
                    // TODO: Optionally this should go to to defaultDestDir.
                    //       1) Find common deepest ancestor dir.
                    //       2) From each dest path, substract the differentiating subpath.
                    //       3) Create the subdirs in defaultDestDir and save there.

                    concatFiles(sortedPaths, concatenatedFilePath);
                    concatenatedFiles.add(concatenatedFilePath);
                }

                return concatenatedFiles;
        }
    }
}

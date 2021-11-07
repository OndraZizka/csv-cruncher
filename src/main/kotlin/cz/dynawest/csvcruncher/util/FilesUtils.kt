package cz.dynawest.csvcruncher.util

import cz.dynawest.csvcruncher.Cruncher
import cz.dynawest.csvcruncher.CruncherInputSubpart
import cz.dynawest.csvcruncher.CsvCruncherException
import cz.dynawest.csvcruncher.ImportArgument
import cz.dynawest.csvcruncher.Options2
import cz.dynawest.csvcruncher.app.Options
import cz.dynawest.csvcruncher.app.Options.*
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.event.Level
import java.io.*
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Types
import java.util.*
import java.util.function.Consumer
import java.util.regex.Pattern
import java.util.stream.Collectors
import javax.json.Json
import javax.json.JsonObjectBuilder

/**
 * TODO: Convert the concat related methods to a context-based class.
 */
//@Suppress("NAME_SHADOWING")
object FilesUtils {
    private const val CONCAT_WORK_SUBDIR_NAME = "concat"
    private val log = logger()

    /**
     * Concatenates given files into a file in the resultPath, named "CsvCruncherConcat.csv".
     * If some of the input files does not end with a new line, it is appended after that file.
     * @return The path to the created file.
     */
    fun concatFiles(filesToConcat: List<Path>, resultPath: Path, ignoreFirstLines: Int, ignoreLineRegex: Pattern?): Path {
        val resultFile = resultPath.toFile()
        val ignoreLineMatcher = ignoreLineRegex?.matcher("")
        var headerIncluded = false

        //  try(FileOutputStream resultOS = new FileOutputStream(resultFile);) {
        try {
            FileWriter(resultFile).use { resultWriter ->
                for (pathToConcat in filesToConcat) {
                    //try (FileInputStream fileToConcatIS = new FileInputStream(pathToConcat.toFile())) {
                    //    IOUtils.copy(fileToConcatReader, resultOS);
                    var linesCountDown = ignoreFirstLines
                    BufferedReader(InputStreamReader(FileInputStream(pathToConcat.toFile()))).use { fileToConcatReader ->
                        var line: String?
                        while (null != fileToConcatReader.readLine().also { line = it }) {
                            linesCountDown--
                            ///System.out.printf("LINE: h: %b lcd: %d LINE:  %s\n", headerIncluded, linesCountDown, line); //
                            if (headerIncluded && linesCountDown >= 0) continue
                            if (headerIncluded && null != ignoreLineMatcher && ignoreLineMatcher.reset(line).matches()) continue
                            headerIncluded = headerIncluded or true // Take the very first line.
                            ///System.out.println("MADE IT...");
                            resultWriter.append(line).append("\n")
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
        } catch (ex: Exception) {
            throw CsvCruncherException("Failed concatenating files into " + resultPath + ": " + ex.message, ex)
        }
        return resultFile.toPath()
    }

    @JvmStatic
    fun sortImports(importArguments: List<ImportArgument>, sortMethod: SortInputPaths?): List<ImportArgument> {
        @Suppress("NAME_SHADOWING")
        var inputPaths = importArguments.map { it.path!! }

        inputPaths = sortInputPaths(inputPaths, sortMethod)
        // Poor man's sorting :) Improve later
        return inputPaths.map { inputPath -> importArguments.find { it.path == inputPath }!! } .toList()
    }

    private fun sortInputPaths(inputPaths_: List<Path>, sortMethod: SortInputPaths?): List<Path> {
        var inputPaths = inputPaths_
        inputPaths = when (sortMethod) {
            SortInputPaths.PARAMS_ORDER -> Collections.unmodifiableList(inputPaths)
            SortInputPaths.ALPHA -> {
                inputPaths = ArrayList(inputPaths)
                Collections.sort(inputPaths)
                inputPaths
            }
            SortInputPaths.TIME -> throw UnsupportedOperationException("Sorting by time not implemented yet.")
            else -> throw UnsupportedOperationException("Unknown sorting method.")
        }
        return inputPaths
    }

    /**
     * Writes the given resultset to a JSON file at given path, one entry per line, optionally as an JSON array.
     */
    @JvmStatic
    fun convertResultToJson(resultSet: ResultSet, destFile: Path, printAsArray: Boolean) {
        try {
            BufferedOutputStream(FileOutputStream(destFile.toFile())).use { outS ->
                OutputStreamWriter(outS, StandardCharsets.UTF_8).use { outW ->
                    val metaData = resultSet.metaData

                    // Cache which cols are numbers.

                    //boolean[] colsAreNumbers = cacheWhichColumnsNeedJsonQuotes(metaData);
                    if (printAsArray) outW.append("[\n")
                    while (resultSet.next()) {
                        // javax.json way
                        val builder = Json.createObjectBuilder()

                        // Columns
                        for (colIndex in 1..metaData.columnCount) {
                            addTheRightTypeToJavaxJsonBuilder(resultSet, colIndex, builder)
                        }
                        val jsonObject = builder.build()
                        val writer = Json.createWriter(outW)
                        writer.writeObject(jsonObject)

                        outW.append(if (printAsArray) ",\n" else "\n")
                    }
                    if (printAsArray) outW.append("]\n")
                }
            }
        } catch (ex: Exception) {
            throw CsvCruncherException("Failed browsing the final query results: " + ex.message, ex)
        }
    }

    /**
     * Used in case we use javax.json.JsonBuilder.
     * This also needs JsonProviderImpl.
     */
    @Throws(SQLException::class)
    private fun addTheRightTypeToJavaxJsonBuilder(resultSet: ResultSet, colIndex: Int, builder: JsonObjectBuilder) {
        val metaData = resultSet.metaData
        var columnLabel = metaData.getColumnLabel(colIndex)
        if (columnLabel.matches("[A-Z][A-Z_]*".toRegex())) columnLabel = columnLabel.lowercase()
        if (resultSet.getObject(colIndex) == null) {
            builder.addNull(columnLabel)
            return
        }
        when (metaData.getColumnType(colIndex)) {
            Types.VARCHAR, Types.CHAR, Types.CLOB -> builder.add(columnLabel, resultSet.getString(colIndex))
            Types.TINYINT, Types.BIT -> builder.add(columnLabel, resultSet.getByte(colIndex).toInt())
            Types.SMALLINT -> builder.add(columnLabel, resultSet.getShort(colIndex).toInt())
            Types.INTEGER -> builder.add(columnLabel, resultSet.getInt(colIndex))
            Types.BIGINT -> builder.add(columnLabel, resultSet.getLong(colIndex))
            Types.BOOLEAN -> builder.add(columnLabel, resultSet.getBoolean(colIndex))
            Types.FLOAT, Types.DOUBLE -> builder.add(columnLabel, resultSet.getDouble(colIndex))
            Types.DECIMAL, Types.NUMERIC -> builder.add(columnLabel, resultSet.getBigDecimal(colIndex))
            Types.DATE -> builder.add(columnLabel, "" + resultSet.getDate(colIndex))
            Types.TIME -> builder.add(columnLabel, "" + resultSet.getTime(colIndex))
            Types.TIMESTAMP -> builder.add(columnLabel, ("" + resultSet.getTimestamp(colIndex)).replace(' ', 'T'))
        }
        // This should be handled by getObject(), but just in case...
        if (resultSet.wasNull()) {
            builder.addNull(columnLabel)
        }
    }

    /**
     * Expands the input paths if they are directories, sorts the resulting groups, filters by the options includes/excludes.
     *
     * @return Input files grouped by the given input paths. CSV headers are not yet checked.
     */
    @JvmStatic
    fun expandFilterSortInputFilesGroups(inputPaths: List<Path>, options: Options2): Map<Path?, List<Path>> {
        if (CombineInputFiles.NONE == options.combineInputFiles) {
            // No splitting - return a list with the same item.
            return mapOfIdentityToSingletonList(inputPaths)
        }

        // Expand the directories.
        var fileGroupsToCombine: Map<Path?, List<Path>> = expandDirectories(inputPaths, options)

        // Filter
        fileGroupsToCombine = filterFileGroups(fileGroupsToCombine, options)
        logFileGroups(fileGroupsToCombine, Level.DEBUG, "Filtered file groups:")

        // If there is just one catch-all group...
        if (fileGroupsToCombine.size == 1 && fileGroupsToCombine.keys.iterator().next() == null) {
            val paths = fileGroupsToCombine[null]!!
            if (paths.isEmpty()) {
                log.info("   *** No files found.")
                return emptyMap()
            }
            if (paths.size == 1) {
                return mapOfIdentityToSingletonList(paths)
            }

            // If there is only one input path, use it as the originating path. Maybe it should be done in combine()?
            if (inputPaths.size == 1) {
                fileGroupsToCombine.remove(null)
                fileGroupsToCombine[inputPaths[0]] = paths
            }
        }
        fileGroupsToCombine = sortFileGroups(options, fileGroupsToCombine)
        logFileGroups(fileGroupsToCombine, Level.DEBUG, "Sorted and filtered file groups:")
        return fileGroupsToCombine
    }

    /**
     * Combine the input files (typically, concatenate).
     * If the paths are directories, they may be combined per each directory, per input dir, per input subdir, or all into one.
     * The combined input files will be witten under the respective "group root directory".
     * For COMBINE_ALL_FILES, the combined file will be written under current user directory ("user.dir").
     *
     * @return Mapping from the concatenated file to the files that ended up in it.
     */
    @JvmStatic
    @Throws(IOException::class)
    fun combineInputFiles(fileGroupsToCombine: Map<Path?, List<Path>>, options: Options2): List<CruncherInputSubpart> {
        // Split into subgroups by column names in the CSV header.
        @Suppress("NAME_SHADOWING") var fileGroupsToCombine = fileGroupsToCombine
        val splitResult: FileGroupsSplitBySchemaResult = splitToSubgroupsPerSameHeaders(fileGroupsToCombine)
        fileGroupsToCombine = splitResult.fileGroupsToCombine
        logFileGroups(fileGroupsToCombine, Level.DEBUG, "File groups split per header structure:")

        // At this point, the group keys are the original group + _<counter>.
        // TODO: Again, refactor this to something more sane.

        // Get the final concatenated file path. Currently, "-out" + _concat.
        val destDir = Paths.get(options.mainOutputDir.toString() + "_" + CONCAT_WORK_SUBDIR_NAME)
        when (options.combineInputFiles) {
            CombineInputFiles.INTERSECT, CombineInputFiles.EXCEPT -> throw UnsupportedOperationException("INTERSECT and EXCEPT combining is not implemented yet.")
            CombineInputFiles.CONCAT -> {
                log.debug("Concatenating input files.")
                return concatenateFilesFromFileGroups(options, fileGroupsToCombine, destDir)
            }
            CombineInputFiles.NONE -> {}
        }
        throw IllegalStateException("Did we miss some CombineInputFiles choice?")
    }

    /** For each path, creates an entry in a map from that path to a singleton list of that path. */
    private fun mapOfIdentityToSingletonList(inputPaths: List<Path>): Map<Path?, List<Path>> {
        return inputPaths.stream().collect(Collectors.toMap({ x -> x }, { o: Path? -> listOf(o!!) }))
        //return inputPaths.associate { it to listOf(it) }
    }

    private fun logFileGroups(fileGroupsToConcat: Map<Path?, List<Path>>, @Suppress("SameParameterValue") level: Level, label: String) {
        // TBD: Apply level.
        /*SubstituteLoggingEvent event = new SubstituteLoggingEvent();
        event.setLevel(level);
        event.setMessage();
        ((Logger) log).log(event);*/
        log.debug("--- $label ---")
        for ((key, value) in fileGroupsToConcat) {
            val msg = """\n * Path: $key: ${value.stream().map { path: Path -> "\n\t- $path" }.collect(Collectors.joining())}"""
            log.debug(msg)
        }
    }

    /**
     * Walks through the directories given in inputPaths and expands them into the contained files,
     * into groups as per rules given by options - see [Options.CombineDirectories], [Options.skipNonReadable].
     *
     * @return A map with one entry per group, containing the files.
     */
    @JvmStatic
    fun expandDirectories(inputPaths: List<Path>, options: Options2): MutableMap<Path?, MutableList<Path>> {
        val fileGroupsToConcat: MutableMap<Path?, MutableList<Path>> = HashMap()
        // null will be used as a special key for COMBINE_ALL_FILES.
        fileGroupsToConcat[null] = ArrayList()
        for (inputPath in inputPaths) try {
            log.debug(" * About to concat $inputPath")

            // Put files simply to "global" group. Might be improved in the future.
            check(inputPath.toFile().exists()) { "File does not exist: $inputPath" }

            // Put files simply to "global" group. Might be improved in the future.
            if (inputPath.toFile().isFile) fileGroupsToConcat[null]!!.add(inputPath)
            // Walk directories for CSV, and group them as per options.combineDirs.
            if (inputPath.toFile().isDirectory) {
                var fileToGroupSorter: Consumer<Path>?
                fileToGroupSorter = when (options.combineDirs) {
                    CombineDirectories.COMBINE_ALL_FILES -> {
                        val fileGroup = fileGroupsToConcat[null]!!
                        Consumer { e: Path -> fileGroup.add(e) }
                    }
                    CombineDirectories.COMBINE_PER_INPUT_DIR -> {
                        val fileGroup = fileGroupsToConcat[inputPath]!!
                        Consumer { e: Path -> fileGroup.add(e) }
                    }
                    CombineDirectories.COMBINE_PER_EACH_DIR -> {
                        //List<Path> fileGroup = fileGroupsToCombine.get(inputPath);
                        Consumer { curFile: Path -> fileGroupsToConcat.computeIfAbsent(curFile.toAbsolutePath().parent) { ArrayList() }.add(curFile) }
                    }
                    CombineDirectories.COMBINE_PER_INPUT_SUBDIR -> {
                        throw UnsupportedOperationException("Not yet implemented") // TODO
                    }
                }
                log.trace("   *** About to walk$inputPath")
                Files.walk(inputPath)
                        .filter { curFile: Path -> Files.isRegularFile(curFile) && curFile.fileName.toString().endsWith(Cruncher.FILENAME_SUFFIX_CSV) } ///.peek(path -> System.out.println("fileToGroupSorter " + path))
                        .filter { file: Path ->
                            if (file.toFile().canRead()) return@filter true
                            if (options.skipNonReadable) {
                                log.info("Skipping non-readable file: $file")
                                return@filter false
                            }
                            throw IllegalArgumentException("Unreadable file (try --skipNonReadable): $file")
                        }
                        .forEach(fileToGroupSorter)
                log.trace("   *** After walking: $fileGroupsToConcat")
            }
        } catch (ex: Exception) {
            throw CsvCruncherException("Failed combining the input files in $inputPath: ${ex.message}", ex)
        }
        return fileGroupsToConcat
    }

    /**
     * Reduces the groups to only contain files that match the include and don't match the exclude pattern - see [Options.includePathsRegex].
     * Also, skips the empty groups.
     */
    @JvmStatic
    fun filterFileGroups(fileGroupsToConcat: Map<Path?, List<Path>>, options: Options2): MutableMap<Path?, List<Path>> {
        val fileGroupsToConcat2: MutableMap<Path?, List<Path>> = HashMap()
        for ((origin, paths) in fileGroupsToConcat) {
            val filteredPaths = filterPaths(options, paths)
            if (paths.isEmpty()) {
                if (origin != null) log.info("   *** No files found in $origin.")
                continue
            }
            fileGroupsToConcat2[origin] = filteredPaths
        }
        return fileGroupsToConcat2
    }

    @JvmStatic
    fun filterPaths(options: Options2, paths: List<Path>): List<Path> {
        if (options.includePathsRegex == null && options.excludePathsRegex == null)
            return paths

        return paths.stream()
                .filter { path: Path -> options.includePathsRegex == null || options.includePathsRegex!!.matcher(path.toString()).matches() }
                .filter { path: Path -> options.excludePathsRegex == null || !options.excludePathsRegex!!.matcher(path.toString()).matches() }
                .collect(Collectors.toList())
    }

    /**
     * Sorts the files within the groups by the configured sorting - see [Options.SortInputPaths]. Skips the empty groups.
     * @return A map with one entry per group, containing the files in sorted order.
     */
    private fun sortFileGroups(options: Options2, fileGroupsToConcat: Map<Path?, List<Path>>): MutableMap<Path?, List<Path>> {
        check(SortInputPaths.PARAMS_ORDER != options.sortInputFileGroups) { "Input file groups have to be sorted somehow, " + SortInputPaths.PARAMS_ORDER.optionValue + " not applicable." }
        val fileGroupsToConcat2: MutableMap<Path?, List<Path>> = HashMap()
        for ((origin, value) in fileGroupsToConcat) {
            val sortedPaths = sortInputPaths(value, options.sortInputFileGroups)
            fileGroupsToConcat2[origin] = sortedPaths
            val dirLabel = if (origin == null) "all files" else "" + origin
            log.debug("   *** Will combine files from " + dirLabel + ": "
                    + sortedPaths.stream().map { path: Path -> "\n\t* $path" }.collect(Collectors.joining()))
        }
        return fileGroupsToConcat2
    }

    /**
     * Some groups may contain CSV files which have different headers;
     * this splits such groups into subgroups and puts them separatedly to returned fileGroups.
     * Takes the order into account - a group is created each time the headers change, even if they change to previously seen structure.
     * The new subgroups names are the <original group path> + _X, where X is an incrementing number.
     *
     * @return A map with one entry per group, containing the files in the original order, but split per CSV header structure.
    </original> */
    @Throws(IOException::class)
    private fun splitToSubgroupsPerSameHeaders(fileGroupsToConcat: Map<Path?, List<Path>>): FileGroupsSplitBySchemaResult {
        // TODO: Record information about what groups were splitted and to what subgroups.
        val fileGroupsToConcat2: MutableMap<Path?, List<Path>> = LinkedHashMap()
        val splittedGroupsInfo_oldGroupToNewGroups: MutableMap<Path?, MutableList<Path>> = LinkedHashMap()
        val result = FileGroupsSplitBySchemaResult(fileGroupsToConcat2, splittedGroupsInfo_oldGroupToNewGroups)
        // TODO: Refactor this into proper models. InputGroup, PerHeaderInputSubgroup, etc.
        for ((originalGroupPath, value) in fileGroupsToConcat) {
            // Check if all files have the same columns header.
            val subGroups_headerStructureToFiles: MutableMap<List<String>, MutableList<Path>> = LinkedHashMap()
            for (fileToConcat in value) {
                val headers = parseColumnsFromFirstCsvLine(fileToConcat.toFile())
                subGroups_headerStructureToFiles.computeIfAbsent(headers) { ArrayList() }.add(fileToConcat)
            }
            if (subGroups_headerStructureToFiles.size == 1) {
                fileGroupsToConcat2[originalGroupPath] = value
            } else {
                // Replaces the original group with few subgroups, with paths suffixed with counter: originalPath_1, originalPath_2, ...
                var counter = 1
                for (filesWithSameHeaders in subGroups_headerStructureToFiles.values) {
                    val subgroupKey = Paths.get("" + originalGroupPath + "_" + counter++)
                    fileGroupsToConcat2.putIfAbsent(subgroupKey, filesWithSameHeaders)
                    // Keep information about what groups were splitted and to what subgroups.
                    splittedGroupsInfo_oldGroupToNewGroups
                            .computeIfAbsent(originalGroupPath) { ArrayList() }
                            .add(subgroupKey)
                }
            }
        }
        return result
    }

    /**
     * @param options Used to filter the files, like "skip 1st line" or regex line matches.
     * @param fileGroupsToConcat Mapping from file group "key" (original group path + counter suffix) to the list of files to be concatenated.
     * @param tmpConcatDir  A directory where the concatenation results shoul go.
     * @return Mapping from the resulting concatenated file to the files that were concatenated.
     */
    @Throws(IOException::class)
    private fun concatenateFilesFromFileGroups(options: Options2, fileGroupsToConcat: Map<Path?, List<Path>>, tmpConcatDir: Path): List<CruncherInputSubpart> {
        val inputSubparts: MutableList<CruncherInputSubpart> = ArrayList()
        val usedConcatFilePaths: MutableSet<Path> = HashSet()
        for (fileGroup in fileGroupsToConcat.entries) {
            // Destination directory
            //log.debug("    Into dest dir: " + tmpConcatDir);
            Files.createDirectories(tmpConcatDir)
            val concatFileName = deriveNameForCombinedFile(fileGroup, usedConcatFilePaths)
            val concatenatedFilePath = tmpConcatDir.resolve(concatFileName)
            usedConcatFilePaths.add(concatenatedFilePath)
            log.debug("Into dest file: $concatenatedFilePath\n   Will combine these files: "
                + fileGroup.value.map { path -> "\n|\t* $path" }.joinToString())

            // TODO: Optionally this could be named better:
            //       1) Find common deepest ancestor dir.
            //       2) From each dest path, substract the differentiating subpath.
            //       3) Create the subdirs in tmpConcatDir and save there.

            // Combine the file sets.
            concatFiles(fileGroup.value, concatenatedFilePath, options.ignoreFirstLines, options.ignoreLineRegex)
            val inputPart = CruncherInputSubpart(
                    originalInputPath = fileGroup.key,
                    combinedFile = concatenatedFilePath,
                    combinedFromFiles = fileGroup.value,
            )
            inputSubparts.add(inputPart)
        }
        return inputSubparts
    }

    /**
     * Come up with some good name for the combined file.
     * If the name was used, append an incrementing number until it is unique.
     *
     * @param fileGroup  A group of files to combine in the value, and the originating input path in the key.
     *                   A null key means assorted files.
     */
    @JvmStatic
    fun deriveNameForCombinedFile(fileGroup: Map.Entry<Path?, List<Path>>, usedConcatFilePaths: MutableSet<Path>): String {
        var originPath = fileGroup.key
        return if (originPath == null) {
            // Assorted files will be combined into resultDir/concat.csv.
            Paths.get("concat" + Cruncher.FILENAME_SUFFIX_CSV).toString()
        } else {
            val pathStr = StringUtils.appendIfMissing(originPath.toString(), Cruncher.FILENAME_SUFFIX_CSV)
            originPath = Paths.get(pathStr)
            val concatFilePath = getNonUsedName(originPath, usedConcatFilePaths)
            usedConcatFilePaths.add(concatFilePath)
            concatFilePath.fileName.toString()
        }
    }

    /**
     * Returns a path derived from path, that is not already used (present in the usedPaths set).
     * The paths derived have the file base name + "_{counter}".
     * Eg. given "some/output/file.csv", if already used, tries "some/output/file_1.csv", and so on.
     * It does NOT add the used path to the usedPaths set.
     */
    @JvmStatic
    fun getNonUsedName(path: Path, usedPaths: MutableSet<Path>): Path {
        @Suppress("NAME_SHADOWING")
        var path = path
        if (!usedPaths.contains(path)) return path

        var suffix = StringUtils.substringAfterLast("" + path.fileName, ".")
        if (!StringUtils.isEmpty(suffix)) suffix = ".$suffix"

        val pathWithoutSuffix = StringUtils.removeEnd(path.toString(), suffix)
        var counter = 0
        do {
            path = Paths.get(pathWithoutSuffix + "_" + ++counter + suffix)
        } while (usedPaths.contains(path))

        usedPaths.add(path)
        return path
    }

    /**
     * Parse the first line of given file, ignoring the initial #'s, NOT respecting quotes and escaping.
     *
     * @return A list of column names in the order from the file.
     */
    @JvmStatic
    @Throws(IOException::class)
    fun parseColumnsFromFirstCsvLine(file: File): List<String> {
        val cols = ArrayList<String>()
        val lineIterator = FileUtils.lineIterator(file)

        // Skip comment lines, starting with ##
        var line: String?
        do {
            if (!lineIterator.hasNext())
                throw IllegalStateException("No first line with columns definition (format: [# ] <colName> [, ...]) in: " + file.path)
            line = lineIterator.nextLine().trim { it <= ' ' }
        } while (line!!.startsWith(CSV_COMMENT_PREFIX))

        /*  I could employ CSVReader if needed.
            CSVReader csvReader = new CSVReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String[] header = csvReader.readNext();
            csvReader.close();
         */
        line = StringUtils.stripStart(line, "#")

        val colNames = StringUtils.splitPreserveAllTokens(line, ",;")
        for (colName in Arrays.asList(*colNames)) {
            @Suppress("NAME_SHADOWING")
            val colName = colName.trim { it <= ' ' }
            check(!colName.isEmpty()) {
                "Empty column name (separators: ,; ) in: ${file.path}\n  The line was: $line"
            }
            /* Removed for #17 and #39.
            check(Cruncher.REGEX_SQL_COLUMN_VALID_NAME.matcher(colName).matches()) {
                "Colname '$colName' must be valid SQL identifier, i.e. must match /${Cruncher.REGEX_SQL_COLUMN_VALID_NAME.pattern()}/i in: ${file.path}"
            }*/
            cols.add(colName)
        }
        return cols
    }

    /**
     * Checks whether the paths point to existing files.
     */
    @JvmStatic
    fun validateInputFiles(inputSubparts: List<CruncherInputSubpart>) {
        val inputPaths: List<Path> = ArrayList(inputSubparts.stream().map { x: CruncherInputSubpart -> x.combinedFile }.collect(Collectors.toList()))
        val notFiles = inputPaths.stream().filter { path: Path -> !path.toFile().isFile }.collect(Collectors.toList())
        if (!notFiles.isEmpty()) {
            val msg = "Some input paths do not point to files: " + notFiles.stream().map { obj: Path -> obj.toString() }.collect(Collectors.joining(", "))
            throw IllegalStateException(msg)
        }
    }

    /**
     * Result of a split into subgroups.
     * When combining CSV files, if they do not match in structure (which is derived from the header names),
     * they are split into subgroups.
     *
     * TODO: Refactor. Previously this was done into a flat structure, but it seems at least 2 levels will be needed.
     * For now I am only putting it to two flat maps "joined" by the subgroup names.
     */
    //@Getter
    //@AllArgsConstructor
    internal class FileGroupsSplitBySchemaResult(
        val fileGroupsToCombine: Map<Path?, List<Path>> = LinkedHashMap(),

        /**
         * Old group to new subgroups.
         */
        val splittedGroupsInfo: Map<Path?, List<Path>> = LinkedHashMap(),
    )

    val CSV_COMMENT_PREFIX = "###"
}
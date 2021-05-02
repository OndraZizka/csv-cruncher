package cz.dynawest.csvcruncher.util

import cz.dynawest.csvcruncher.*
import cz.dynawest.csvcruncher.util.FilesUtils.combineInputFiles
import cz.dynawest.csvcruncher.util.FilesUtils.deriveNameForCombinedFile
import cz.dynawest.csvcruncher.util.FilesUtils.expandDirectories
import cz.dynawest.csvcruncher.util.FilesUtils.expandFilterSortInputFilesGroups
import cz.dynawest.csvcruncher.util.FilesUtils.filterFileGroups
import cz.dynawest.csvcruncher.util.FilesUtils.filterPaths
import cz.dynawest.csvcruncher.util.FilesUtils.getNonUsedName
import cz.dynawest.csvcruncher.util.FilesUtils.parseColumnsFromFirstCsvLine
import org.junit.Assert
import org.junit.Test
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.function.Consumer
import java.util.regex.Pattern

class FilesUtilsTest {
    @Test
    fun filterFilePaths() {
        val paths: MutableList<Path> = ArrayList()
        paths.add(Paths.get("foo.bar"))
        paths.add(Paths.get("foo.foo"))
        paths.add(Paths.get("bar.foo"))
        paths.add(Paths.get("bar.bar"))
        val options = Options()
        options.includePathsRegex = Pattern.compile("^foo\\..*")
        options.excludePathsRegex = Pattern.compile(".*\\.bar$")
        var paths1: List<Path?> = filterPaths(options, paths)
        Assert.assertFalse(paths1.contains(Paths.get("foo.bar")))
        Assert.assertFalse(paths1.contains(Paths.get("bar.foo")))
        Assert.assertFalse(paths1.contains(Paths.get("bar.bar")))
        Assert.assertTrue(paths1.contains(Paths.get("foo.foo")))

        // Nulls
        options.includePathsRegex = null
        options.excludePathsRegex = null
        paths1 = filterPaths(options, paths)
        Assert.assertTrue(paths1.contains(Paths.get("foo.bar")))
        Assert.assertTrue(paths1.contains(Paths.get("bar.foo")))
        Assert.assertTrue(paths1.contains(Paths.get("bar.bar")))
        Assert.assertTrue(paths1.contains(Paths.get("foo.foo")))
        options.includePathsRegex = null
        options.excludePathsRegex = Pattern.compile(".*\\.bar$")
        paths1 = filterPaths(options, paths)
        Assert.assertFalse(paths1.contains(Paths.get("foo.bar")))
        Assert.assertTrue(paths1.contains(Paths.get("bar.foo")))
        Assert.assertFalse(paths1.contains(Paths.get("bar.bar")))
        Assert.assertTrue(paths1.contains(Paths.get("foo.foo")))
    }

    @Test
    fun concatFiles() {
    }

    @Test
    fun sortInputPaths() {
    }

    @Test
    fun convertResultToJson() {
    }

    @Test
    fun deriveNameForCombinedFile() {
        val fileGroup = HashMap<Path?, List<Path>>()
        fileGroup[Paths.get("foo")] = emptyList()
        val usedConcatFilePaths = HashSet<Path>()
        usedConcatFilePaths.add(Paths.get("foo.csv"))
        val derivedName = deriveNameForCombinedFile(fileGroup.entries.iterator().next(), usedConcatFilePaths)
        Assert.assertEquals("foo_1.csv", derivedName)
    }

    @Test
    fun deriveNameForCombinedFile_dir() {
        val fileGroup = HashMap<Path?, List<Path>>()
        fileGroup[Paths.get("foo/bar.csv")] = emptyList()
        val usedConcatFilePaths = HashSet<Path>()
        usedConcatFilePaths.add(Paths.get("foo/bar.csv"))
        val derivedName = deriveNameForCombinedFile(fileGroup.entries.iterator().next(), usedConcatFilePaths)
        Assert.assertEquals("bar_1.csv", derivedName)
    }

    @get:Test
    val nonUsedName: Unit
        get() {
            var nonUsed: Path
            run {
                val path = Paths.get("some/path.csv")
                val path_1 = Paths.get("some/path_1.csv")
                val path_2 = Paths.get("some/path_2.csv")
                val path2 = Paths.get("some/path2.csv")
                val path3 = Paths.get("some/path3.csv")
                nonUsed = getNonUsedName(path, mutableSetOf<Path>())
                Assert.assertEquals(path, nonUsed)
                nonUsed = getNonUsedName(path, HashSet(setOf(clonePath(path))))
                Assert.assertEquals(path_1, nonUsed)
                nonUsed = getNonUsedName(path, HashSet(Arrays.asList(path, path_1)))
                Assert.assertEquals(path_2, nonUsed)
            }
            run {
                val path = Paths.get("some/path")
                val path_1 = Paths.get("some/path_1")
                val path_2 = Paths.get("some/path_2")
                val path2 = Paths.get("some/path2")
                val path3 = Paths.get("some/path3")
                nonUsed = getNonUsedName(path, mutableSetOf<Path>())
                Assert.assertEquals(path, nonUsed)
                nonUsed = getNonUsedName(path, HashSet(setOf(clonePath(path))))
                Assert.assertEquals(path_1, nonUsed)
                nonUsed = getNonUsedName(path, HashSet(Arrays.asList(clonePath(path), path_1)))
                Assert.assertEquals(path_2, nonUsed)
            }
        }

    private fun clonePath(path: Path): Path {
        return path.resolve("./").normalize()
    }

    @Test
    @Throws(IOException::class)
    fun combineInputFiles_changedSchema() {
        val options = Options()
        options.inputPaths = Arrays.asList(testDataDir!!.resolve("sample-changedSchema").toString())
        options.excludePathsRegex = Pattern.compile(".*/LOAD.*\\.csv")
        options.combineDirs = Options.CombineDirectories.COMBINE_ALL_FILES
        options.combineInputFiles = Options.CombineInputFiles.CONCAT
        options.outputPathCsv = testOutputDir!!.resolve("combineInputFilesTest.csv").toString()
        options.overwrite = true
        options.initialRowNumber = 1L
        options.sql = "SELECT * FROM concat"
        val inputPaths = listOf(testDataDir!!.resolve("sample-changedSchema"))
        val inputFileGroups = expandFilterSortInputFilesGroups(inputPaths, options)
        val inputSubparts = combineInputFiles(inputFileGroups, options)
        Assert.assertNotNull(inputSubparts)
        Assert.assertEquals(2, inputSubparts.size.toLong())
        inputSubparts.forEach(Consumer { inputSubpart: CruncherInputSubpart ->
            Assert.assertTrue(inputSubpart.combinedFile.toFile().isFile)
            Assert.assertTrue(inputSubpart.combinedFile.toFile().length() > 0)
            inputSubpart.combinedFromFiles!!.forEach { sourceFile -> sourceFile.toFile().isFile() }
        })
    }

    @Test
    fun expandDirectories() {
        val options = Options()
        val inputPaths = Arrays.asList(testDataDir!!.resolve("sample-changedSchema"))
        options.inputPaths = mutableListOf(inputPaths[0].toString())
        options.includePathsRegex = Pattern.compile(".*\\.csv")
        options.excludePathsRegex = Pattern.compile(".*/LOAD.*\\.csv")
        options.combineDirs = Options.CombineDirectories.COMBINE_ALL_FILES
        var fileGroupsToConcat: Map<Path?, List<Path>> = expandDirectories(inputPaths, options)
        Assert.assertEquals("Just one catchall group expected", 1, fileGroupsToConcat.size.toLong())
        Assert.assertNotNull("Just one catchall group expected", fileGroupsToConcat[null])
        Assert.assertEquals("5 files found", 5, fileGroupsToConcat[null]!!.size.toLong())
        fileGroupsToConcat = filterFileGroups(fileGroupsToConcat, options)
        Assert.assertEquals("Just one catchall group expected", 1, fileGroupsToConcat.size.toLong())
        Assert.assertNotNull("Just one catchall group expected", fileGroupsToConcat[null])
        Assert.assertEquals("4 files found", 4, fileGroupsToConcat[null]!!.size.toLong())
    }

    @Test
    @Throws(IOException::class)
    fun parseColsFromFirstCsvLine() {
        val csvFileWithHeader = testDataDir!!.resolve("sample-collab/session_telephony_pins/20180918-132721852.csv")
        val colNames = parseColumnsFromFirstCsvLine(csvFileWithHeader.toFile())

        // Op,id,uuid,session_id,pin,pin_type,pin_access_type,enrollment_id,created_time,modified_time
        Assert.assertEquals("Proper column count", 10, colNames.size.toLong())
        Assert.assertEquals("Col name Op", "Op", colNames[0])
        Assert.assertEquals("Col name id", "id", colNames[1])
        Assert.assertEquals("Col name uuid", "uuid", colNames[2])
        Assert.assertEquals("Col name modified_time", "modified_time", colNames[9])
    }

    companion object {
        var testDataDir = CsvCruncherTestUtils.testDataDir
        var testOutputDir = CsvCruncherTestUtils.testOutputDir
    }
}
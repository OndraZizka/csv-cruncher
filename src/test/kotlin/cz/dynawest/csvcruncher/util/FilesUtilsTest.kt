@file:Suppress("LocalVariableName")

package cz.dynawest.csvcruncher.util

import cz.dynawest.csvcruncher.*
import cz.dynawest.csvcruncher.app.OptionsEnums
import cz.dynawest.csvcruncher.util.FilesUtils.combineInputFiles
import cz.dynawest.csvcruncher.util.FilesUtils.deriveNameForCombinedFile
import cz.dynawest.csvcruncher.util.FilesUtils.expandDirectories
import cz.dynawest.csvcruncher.util.FilesUtils.expandFilterSortInputFilesGroups
import cz.dynawest.csvcruncher.util.FilesUtils.filterFileGroups
import cz.dynawest.csvcruncher.util.FilesUtils.filterPaths
import cz.dynawest.csvcruncher.util.FilesUtils.getNonUsedName
import cz.dynawest.csvcruncher.util.FilesUtils.parseColumnsFromFirstCsvLine
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
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
        val options = Options2()
        options.includePathsRegex = Pattern.compile("^foo\\..*")
        options.excludePathsRegex = Pattern.compile(".*\\.bar$")
        var paths1: List<Path?> = filterPaths(options, paths)
        assertFalse(paths1.contains(Paths.get("foo.bar")))
        assertFalse(paths1.contains(Paths.get("bar.foo")))
        assertFalse(paths1.contains(Paths.get("bar.bar")))
        assertTrue(paths1.contains(Paths.get("foo.foo")))

        // Nulls
        options.includePathsRegex = null
        options.excludePathsRegex = null
        paths1 = filterPaths(options, paths)
        assertTrue(paths1.contains(Paths.get("foo.bar")))
        assertTrue(paths1.contains(Paths.get("bar.foo")))
        assertTrue(paths1.contains(Paths.get("bar.bar")))
        assertTrue(paths1.contains(Paths.get("foo.foo")))
        options.includePathsRegex = null
        options.excludePathsRegex = Pattern.compile(".*\\.bar$")
        paths1 = filterPaths(options, paths)
        assertFalse(paths1.contains(Paths.get("foo.bar")))
        assertTrue(paths1.contains(Paths.get("bar.foo")))
        assertFalse(paths1.contains(Paths.get("bar.bar")))
        assertTrue(paths1.contains(Paths.get("foo.foo")))
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
        assertEquals("foo_1.csv", derivedName)
    }

    @Test
    fun deriveNameForCombinedFile_dir() {
        val fileGroup = HashMap<Path?, List<Path>>()
        fileGroup[Paths.get("foo/bar.csv")] = emptyList()
        val usedConcatFilePaths = HashSet<Path>()
        usedConcatFilePaths.add(Paths.get("foo/bar.csv"))
        val derivedName = deriveNameForCombinedFile(fileGroup.entries.iterator().next(), usedConcatFilePaths)
        assertEquals("bar_1.csv", derivedName)
    }

    @Test
    fun test_getNonUsedName(): Unit {
        var nonUsed: Path
        run {
            val path = Paths.get("some/path.csv")
            val path_1 = Paths.get("some/path_1.csv")
            val path_2 = Paths.get("some/path_2.csv")
            nonUsed = getNonUsedName(path, mutableSetOf<Path>())
            assertEquals(path, nonUsed)
            nonUsed = getNonUsedName(path, HashSet(setOf(clonePath(path))))
            assertEquals(path_1, nonUsed)
            nonUsed = getNonUsedName(path, HashSet(Arrays.asList(path, path_1)))
            assertEquals(path_2, nonUsed)
        }
        run {
            val path = Paths.get("some/path")
            val path_1 = Paths.get("some/path_1")
            val path_2 = Paths.get("some/path_2")
            nonUsed = getNonUsedName(path, mutableSetOf<Path>())
            assertEquals(path, nonUsed)
            nonUsed = getNonUsedName(path, HashSet(setOf(clonePath(path))))
            assertEquals(path_1, nonUsed)
            nonUsed = getNonUsedName(path, HashSet(Arrays.asList(clonePath(path), path_1)))
            assertEquals(path_2, nonUsed)
        }
    }

    private fun clonePath(path: Path): Path {
        return path.resolve("./").normalize()
    }

    @Test
    @Throws(IOException::class)
    fun combineInputFiles_changedSchema() {
        val options = Options2()
        options.newImportArgument().apply { path = testDataDir.resolve("sample-changedSchema") }
        options.excludePathsRegex = Pattern.compile(".*/LOAD.*\\.csv")
        options.combineDirs = OptionsEnums.CombineDirectories.COMBINE_ALL_FILES
        options.combineInputFiles = OptionsEnums.CombineInputFiles.CONCAT
        options.newExportArgument().apply {
            path = testOutputDir.resolve("combineInputFilesTest.csv")
            sqlQuery = "SELECT * FROM concat"
        }
        options.overwrite = true
        options.initialRowNumber = 1L

        val inputPaths = listOf(testDataDir.resolve("sample-changedSchema"))
        val inputFileGroups = expandFilterSortInputFilesGroups(inputPaths, options)
        val inputSubparts = combineInputFiles(inputFileGroups, options)
        assertNotNull(inputSubparts)
        assertEquals(2, inputSubparts.size.toLong())
        inputSubparts.forEach(Consumer { inputSubpart: CruncherInputSubpart ->
            assertTrue(inputSubpart.combinedFile.toFile().isFile)
            assertTrue(inputSubpart.combinedFile.toFile().length() > 0)
            inputSubpart.combinedFromFiles!!.forEach { sourceFile -> sourceFile.toFile().isFile() }
        })
    }

    @Test
    fun expandDirectories() {
        val options = Options2()
        val inputPaths = Arrays.asList(testDataDir.resolve("sample-changedSchema"))
        options.newImportArgument().apply { path = inputPaths[0] }
        options.includePathsRegex = Pattern.compile(".*\\.csv")
        options.excludePathsRegex = Pattern.compile(".*/LOAD.*\\.csv")
        options.combineDirs = OptionsEnums.CombineDirectories.COMBINE_ALL_FILES
        var fileGroupsToConcat: Map<Path?, List<Path>> = expandDirectories(inputPaths, options)
        assertEquals(1, fileGroupsToConcat.size.toLong(), "Just one catchall group expected")
        assertNotNull(fileGroupsToConcat[null], "Just one catchall group expected")
        assertEquals(5, fileGroupsToConcat[null]!!.size.toLong(), "5 files found")
        fileGroupsToConcat = filterFileGroups(fileGroupsToConcat, options)
        assertEquals(1, fileGroupsToConcat.size.toLong(), "Just one catchall group expected")
        assertNotNull(fileGroupsToConcat[null], "Just one catchall group expected")
        assertEquals(4, fileGroupsToConcat[null]!!.size.toLong(), "4 files found")
    }

    @Test
    @Throws(IOException::class)
    fun parseColsFromFirstCsvLine() {
        val csvFileWithHeader = testDataDir.resolve("sample-collab/session_telephony_pins/20180918-132721852.csv")
        val colNames = parseColumnsFromFirstCsvLine(csvFileWithHeader.toFile())

        // Op,id,uuid,session_id,pin,pin_type,pin_access_type,enrollment_id,created_time,modified_time
        Assertions.assertEquals(10, colNames.size.toLong(), "Proper column count")
        assertEquals("Op", colNames[0], "Col name Op")
        assertEquals( "id", colNames[1], "Col name id")
        assertEquals("uuid", colNames[2], "Col name uuid")
        assertEquals("modified_time", colNames[9], "Col name modified_time")
    }

    companion object {
        var testDataDir = CsvCruncherTestUtils.testDataDir
        var testOutputDir = CsvCruncherTestUtils.testOutputDir
    }
}
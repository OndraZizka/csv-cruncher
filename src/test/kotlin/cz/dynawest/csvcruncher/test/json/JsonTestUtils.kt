package cz.dynawest.csvcruncher.test.json

import cz.dynawest.csvcruncher.converters.CrunchProperty
import cz.dynawest.csvcruncher.converters.EntryProcessor
import cz.dynawest.csvcruncher.converters.FlattenedEntrySequence
import cz.dynawest.csvcruncher.converters.json.JsonFileFlattener
import cz.dynawest.csvcruncher.util.logger
import cz.dynawest.util.ResourceLoader
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.nio.file.Path

object JsonTestUtils {

    fun prepareEntriesFromFile(testFilePath: String, itemsArraySprout: String = "/"): MutableList<Map<String, CrunchProperty>> {
        val converter = JsonFileFlattener()

        val inputStream: InputStream =
            if(File(testFilePath).exists())
                FileInputStream(testFilePath)
            else
                ResourceLoader.openResourceAtRelativePath(Path.of(testFilePath))

        val entries = mutableListOf<Map<String, CrunchProperty>>()
        converter.visitEntries(inputStream, Path.of(itemsArraySprout), object : EntryProcessor {
            override fun processEntry(entry: FlattenedEntrySequence) {
                val flattenedPropsByName: Map<String, CrunchProperty> = entry.flattenedProperties.associateBy { myProp -> myProp.name }
                entries.add(flattenedPropsByName)

                val countHint = if (flattenedPropsByName.size <= 10) "" else " (10 of ${flattenedPropsByName.size})"
                log.info("Entry$countHint:" + flattenedPropsByName.entries.take(10).map {"${it.key} = ${it.value.toCsvString()}"})
            }
        })
        return entries
    }


    private val log = logger()

    val PROJECT_ROOT_TOKEN = "{project}"
}
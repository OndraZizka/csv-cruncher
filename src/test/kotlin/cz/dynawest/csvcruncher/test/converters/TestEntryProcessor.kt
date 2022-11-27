package cz.dynawest.csvcruncher.test.converters

import cz.dynawest.csvcruncher.converters.CrunchProperty
import cz.dynawest.csvcruncher.converters.EntryProcessor
import cz.dynawest.csvcruncher.converters.FlattenedEntrySequence
import cz.dynawest.csvcruncher.util.logger

class TestEntryProcessor : EntryProcessor {
    val entries = mutableListOf<Map<String, CrunchProperty>>()

    override fun processEntry(entry: FlattenedEntrySequence) {
        val flattenedPropsByName = entry.flattenedProperties.associateBy { myProp -> myProp.name }
        entries.add(flattenedPropsByName)
        log.info("Entry (10 of ${flattenedPropsByName.size}:" + flattenedPropsByName.entries.take(10).map { "\n    ${it.key} = ${it.value}" })
    }
    private val log = logger()
}
package cz.dynawest.csvcruncher.converters

import kotlin.math.max


/**
 * Collects the information about columns; that may be later used to determine the name of the column, the type, etc.
 */
class TabularPropertiesMetadataCollector : EntryProcessor {

    val propertiesSoFar: MutableMap<String, PropertyInfo> = mutableMapOf()

    override fun collectPropertiesMetadata(entry: FlattenedEntrySequence) {
        for (flattenedField in entry.flattenedProperties) {

            val propertyName = flattenedField.name

            propertiesSoFar.compute(propertyName) {
                    name: String, propertyInfo: PropertyInfo? ->
                propertyInfo ?: PropertyInfo(name).apply {
                    when (flattenedField) {
                        is MyProperty.BooleanMyProperty -> { this.types.boolean++; this.maxLength = max(maxLength, 5) }
                        is MyProperty.NullMyProperty -> { this.types.nill++; this.maxLength = max(maxLength, 4) }
                        is MyProperty.NumberMyProperty -> { this.types.number++; this.maxLength = max(maxLength, flattenedField.value.toString().length) }
                        is MyProperty.StringMyProperty -> { this.types.string++; this.maxLength = max(maxLength, flattenedField.value.length) }
                    }
                }
            }
        }
    }
}

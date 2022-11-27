package cz.dynawest.csvcruncher.converters

import kotlin.math.max


/**
 * Collects the information about columns; that may be later used to determine the name of the column, the type, etc.
 * Serves for the 1st pass of the data source.
 */
class TabularPropertiesMetadataCollector : EntryProcessor {

    val propertiesSoFar: MutableMap<String, PropertyInfo> = mutableMapOf()

    override fun processEntry(entry: FlattenedEntrySequence) {
        for (flattenedField in entry.flattenedProperties) {

            val propertyName = flattenedField.name

            propertiesSoFar.compute(propertyName) {
                    name: String, propertyInfo: PropertyInfo? ->
                (propertyInfo ?: PropertyInfo(name))
                .apply {
                    when (flattenedField) {
                        is CrunchProperty.Boolean -> { this.types.boolean++; this.maxLength = max(maxLength, 5) }
                        is CrunchProperty.Null -> { this.types.nill++; this.maxLength = max(maxLength, 4) }
                        is CrunchProperty.Number -> { this.types.number++; this.maxLength = max(maxLength, flattenedField.value.toString().length) }
                        is CrunchProperty.String -> { this.types.string++; this.maxLength = max(maxLength, flattenedField.value.length) }
                        is CrunchProperty.Array -> { this.types.array++; this.maxLength = max(maxLength, 2) } // Just "[]"
                        is CrunchProperty.Object -> { this.types.obj++; this.maxLength = max(maxLength, 2) } // Just "{}"
                        is CrunchProperty.Expression -> { this.types.expr++; this.maxLength = max(maxLength, flattenedField.value.length) }
                    }
                }
            }
        }
    }
}


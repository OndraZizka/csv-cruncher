package cz.dynawest.csvcruncher.converters

/**
 * Abstract internal representation of a property parsed from any source.
 */
sealed class CrunchProperty (open val name: kotlin.String) {

    abstract fun toCsvString(): kotlin.String

    data class Number (override val name: kotlin.String, val value: kotlin.Number): CrunchProperty(name) { override fun toCsvString() = value.toString() }
    data class String (override val name: kotlin.String, val value: kotlin.String): CrunchProperty(name) { override fun toCsvString() = quoteAndEscape(value) }
    data class Boolean(override val name: kotlin.String, val value: kotlin.Boolean): CrunchProperty(name) { override fun toCsvString() = value.toString() }
    data class Null   (override val name: kotlin.String): CrunchProperty(name) { override fun toCsvString() = "NULL" }
    data class Array  (override val name: kotlin.String, val items: List<kotlin.String>): CrunchProperty(name) { override fun toCsvString() = "[...]" }
    data class Object (override val name: kotlin.String, val items: Map<kotlin.String, kotlin.String>): CrunchProperty(name) { override fun toCsvString() = "{...}" }
    /** The value is the expression itself, never the computed value. */
    data class Expression (override val name: kotlin.String, val value: kotlin.String): CrunchProperty(name) { override fun toCsvString() = quoteAndEscape(value) }

    companion object {
        fun quoteAndEscape(string: kotlin.String): kotlin.String {
            return "\"${string.replace("\"", "\\\"")}\""
        }
    }
}
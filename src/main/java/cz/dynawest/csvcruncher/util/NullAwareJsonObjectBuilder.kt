package cz.dynawest.csvcruncher.util

import java.math.BigDecimal
import java.math.BigInteger
import javax.json.JsonArrayBuilder
import javax.json.JsonObject
import javax.json.JsonObjectBuilder
import javax.json.JsonValue

class NullAwareJsonObjectBuilder private constructor(  // Decorated object per Decorator Pattern.
        private val builder: JsonObjectBuilder) : JsonObjectBuilder {
    override fun add(name: String, value: JsonValue?): JsonObjectBuilder {
        builder.add(name, value ?: JsonValue.NULL)
        return this
    }

    override fun add(name: String, value: String?): JsonObjectBuilder {
        if (value == null) builder.addNull(name) else builder.add(name, value)
        return this
    }

    override fun add(name: String, value: BigInteger?): JsonObjectBuilder {
        if (value == null) builder.addNull(name) else builder.add(name, value)
        return this
    }

    override fun add(name: String, value: BigDecimal?): JsonObjectBuilder {
        if (value == null) builder.addNull(name) else builder.add(name, value)
        return this
    }

    override fun add(name: String, value: Int): JsonObjectBuilder {
        builder.add(name, value)
        return this
    }

    override fun add(name: String, value: Long): JsonObjectBuilder {
        builder.add(name, value)
        return this
    }

    override fun add(name: String, value: Double): JsonObjectBuilder {
        builder.add(name, value)
        return this
    }

    override fun add(name: String, value: Boolean): JsonObjectBuilder {
        builder.add(name, value)
        return this
    }

    override fun add(name: String, builder: JsonObjectBuilder): JsonObjectBuilder {
        this.builder.add(name, builder)
        return this
    }

    override fun add(name: String, builder: JsonArrayBuilder): JsonObjectBuilder {
        this.builder.add(name, builder)
        return this
    }

    override fun addNull(name: String): JsonObjectBuilder {
        builder.addNull(name)
        return this
    }

    override fun addAll(builder: JsonObjectBuilder): JsonObjectBuilder {
        this.builder.addAll(builder)
        return this
    }

    override fun remove(name: String): JsonObjectBuilder {
        builder.remove(name)
        return this
    }

    override fun build(): JsonObject {
        return builder.build()
    }

    companion object {
        // Use the Factory Pattern to create an instance.
        fun wrap(builder: JsonObjectBuilder?): JsonObjectBuilder {
            requireNotNull(builder) { "Can't wrap null." }
            return NullAwareJsonObjectBuilder(builder)
        }
    }
}
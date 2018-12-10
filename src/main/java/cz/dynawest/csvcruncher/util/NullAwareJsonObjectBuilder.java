package cz.dynawest.csvcruncher.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

public class NullAwareJsonObjectBuilder implements JsonObjectBuilder
{
    // Decorated object per Decorator Pattern.
    private final JsonObjectBuilder builder;

    // Use the Factory Pattern to create an instance.
    public static JsonObjectBuilder wrap(JsonObjectBuilder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("Can't wrap null.");
        }
        return new NullAwareJsonObjectBuilder(builder);
    }

    private NullAwareJsonObjectBuilder(JsonObjectBuilder builder) {
        this.builder = builder;
    }

    @Override
    public JsonObjectBuilder add(String name, JsonValue value) {
        builder.add(name, (value == null) ? JsonValue.NULL : value);
        return this;
    }

    @Override
    public JsonObjectBuilder add(String name, String value)
    {
        JsonObjectBuilder jsonObjectBuilder = (value == null) ? builder.addNull(name) : builder.add(name, value);
        return this;
    }

    @Override
    public JsonObjectBuilder add(String name, BigInteger value)
    {
        JsonObjectBuilder jsonObjectBuilder = (value == null) ? builder.addNull(name) : builder.add(name, value);
        return this;
    }

    @Override
    public JsonObjectBuilder add(String name, BigDecimal value)
    {
        JsonObjectBuilder jsonObjectBuilder = (value == null) ? builder.addNull(name) : builder.add(name, value);
        return this;
    }

    @Override
    public JsonObjectBuilder add(String name, int value)
    {
        builder.add(name, value);
        return this;
    }

    @Override
    public JsonObjectBuilder add(String name, long value)
    {
        builder.add(name, value);
        return this;
    }

    @Override
    public JsonObjectBuilder add(String name, double value)
    {
        builder.add(name, value);
        return this;
    }

    @Override
    public JsonObjectBuilder add(String name, boolean value)
    {
        builder.add(name, value);
        return this;
    }

    @Override
    public JsonObjectBuilder add(String name, JsonObjectBuilder builder)
    {
        this.builder.add(name, builder);
        return this;
    }

    @Override
    public JsonObjectBuilder add(String name, JsonArrayBuilder builder)
    {
        this.builder.add(name, builder);
        return this;
    }

    @Override
    public JsonObjectBuilder addNull(String name)
    {
        builder.addNull(name);
        return this;
    }

    @Override
    public JsonObjectBuilder addAll(JsonObjectBuilder builder)
    {
        this.builder.addAll(builder);
        return this;
    }

    @Override
    public JsonObjectBuilder remove(String name)
    {
        builder.remove(name);
        return this;
    }

    @Override
    public JsonObject build()
    {
        return builder.build();
    }
}

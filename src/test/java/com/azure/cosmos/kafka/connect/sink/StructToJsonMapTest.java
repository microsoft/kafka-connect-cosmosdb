package com.azure.cosmos.kafka.connect.sink;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StructToJsonMapTest {
    @Test
    public void emptyStructToEmptyMap() {
        Schema schema = SchemaBuilder.struct()
            .build();
        Struct struct = new Struct(schema);
        assertEquals(ImmutableMap.of(), StructToJsonMap.toJsonMap(struct));
    }

    @Test
    public void complextStructToMap() {
        Schema embeddedSchema = SchemaBuilder.struct()
            .field("embedded_string", Schema.STRING_SCHEMA)
            .build();
        Schema schema = SchemaBuilder.struct()
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int8", Schema.INT8_SCHEMA)
            .field("int16", Schema.INT16_SCHEMA)
            .field("int32", Schema.INT32_SCHEMA)
            .field("int64", Schema.INT64_SCHEMA)
            .field("float32", Schema.FLOAT32_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("date", Date.SCHEMA)
            .field("time", Time.SCHEMA)
            .field("timestamp", Timestamp.SCHEMA)
            .field("decimal", Decimal.schema(10))
            .field("string", Schema.STRING_SCHEMA)
            .field("struct", embeddedSchema)
            .field("array_of_boolean", SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build())
            .field("array_of_struct", SchemaBuilder.array(embeddedSchema).build())
            .field("optional_string", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

        java.util.Date now = new java.util.Date();
        BigDecimal ten = new BigDecimal(10).setScale(10);
        String quickBrownFox = "The quick brown fox jumps over the lazy dog";
        Struct struct = new Struct(schema)
            .put("boolean", false)
            .put("int8", (byte) 0)
            .put("int16", (short) 1)
            .put("int32", 2)
            .put("int64", (long) 3)
            .put("float32", (float) 4.0)
            .put("float64", (double) 5.0)
            .put("date", now)
            .put("time", now)
            .put("timestamp", now)
            .put("decimal", ten)
            .put("string", quickBrownFox)
            .put("struct", new Struct(embeddedSchema)
                .put("embedded_string", quickBrownFox))
            .put("array_of_boolean", ImmutableList.of(false))
            .put("array_of_struct", ImmutableList.of(
                new Struct(embeddedSchema).put("embedded_string", quickBrownFox)));

        Map<String, Object> converted = StructToJsonMap.toJsonMap(struct);
        assertEquals(false, converted.get("boolean"));
        assertEquals((byte) 0, converted.get("int8"));
        assertEquals((short) 1, converted.get("int16"));
        assertEquals(2, converted.get("int32"));
        assertEquals((long) 3, converted.get("int64"));
        assertEquals(4.0f, (float) converted.get("float32"), 0.01f);
        assertEquals(5.0d, (double) converted.get("float64"), 0.01d);
        assertEquals(now, converted.get("date"));
        assertEquals(now, converted.get("time"));
        assertEquals(now, converted.get("timestamp"));
        assertEquals(ten, converted.get("decimal"));
        assertEquals(quickBrownFox, converted.get("string"));
        assertEquals(quickBrownFox,
            ((Map<String, Object>) converted.get("struct")).get("embedded_string"));
        assertEquals(false, ((List<Boolean>) converted.get("array_of_boolean")).get(0));
        assertEquals(ImmutableMap.of("embedded_string", quickBrownFox),
            ((List<Struct>) converted.get("array_of_struct")).get(0));
        assertNull(converted.get("optional_string"));
    }
}

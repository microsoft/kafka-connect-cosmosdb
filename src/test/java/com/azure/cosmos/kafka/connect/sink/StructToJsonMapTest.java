// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.HashMap;
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
        assertEquals(Map.of(), StructToJsonMap.toJsonMap(struct));
    }


    @Test
    public void structWithEmptyArrayToMap() {
        Schema schema = SchemaBuilder.struct()
                                     .field("array_of_boolean", SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build());

        Struct struct = new Struct(schema)
            .put("array_of_boolean", List.of());

        Map<String, Object> converted = StructToJsonMap.toJsonMap(struct);
        assertEquals(List.of(), converted.get("array_of_boolean"));
    }

    @Test
    public void complexStructToMap() {
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
                                     .field("map",
                                         SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
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
            .put("array_of_boolean", List.of(false))
            .put("array_of_struct", List.of(
                new Struct(embeddedSchema).put("embedded_string", quickBrownFox)))
            .put("map", Map.of("key", "value"));

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
        assertEquals(quickBrownFox, ((Map<String, Object>) converted.get("struct")).get("embedded_string"));
        assertEquals(false, ((List<Boolean>) converted.get("array_of_boolean")).get(0));
        assertEquals(Map.of("embedded_string", quickBrownFox),
            ((List<Struct>) converted.get("array_of_struct")).get(0));
        assertNull(converted.get("optional_string"));
        assertEquals(Map.of("key", "value"), converted.get("map"));
    }

    @Test
    public void nullableStructTest() {
        Schema embeddedSchema = SchemaBuilder.struct()
                                             .optional()
                                             .field("embedded_string", Schema.STRING_SCHEMA)
                                             .field("embedded_int", Schema.INT32_SCHEMA)
                                             .build();
        Schema schema = SchemaBuilder.struct()
                                     .field("struct", embeddedSchema)
                                     .field("string", Schema.STRING_SCHEMA)
                                     .build();

        Struct struct = new Struct(schema)
            .put("struct", null)
            .put("string", "string_value");

        Map<String, Object> converted = StructToJsonMap.toJsonMap(struct);
        assertEquals("string_value", converted.get("string"));
        assertNull(converted.get("struct"));
    }

    @Test
    public void structWithMap() throws JsonProcessingException {
        Schema schemaWithMapNested = SchemaBuilder.struct()
                                                  .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA,
                                                                                 Schema.STRING_SCHEMA)
                                                                             .build()
                                                  ).build();

        Struct structWithMapNested = new Struct(schemaWithMapNested)
            .put("map", Map.of("key", "value"));


        Map<String, Object> toJsonMap = StructToJsonMap.toJsonMap(structWithMapNested);
        assertEquals(Map.of("key", "value"), toJsonMap.get("map"));
        assertEquals("{\"map\":{\"key\":\"value\"}}", new ObjectMapper().writeValueAsString(toJsonMap));
        assertEquals(Map.of("map", Map.of("key", "value")), toJsonMap);
    }

    @Test
    public void structWithMapWithStruct() throws JsonProcessingException {
        Schema schemaWithMapNested = SchemaBuilder.struct()
                                                  .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA,
                                                      SchemaBuilder.struct()
                                                                   .field("key", Schema.STRING_SCHEMA)
                                                                   .build()
                                                  ).build())
                                                  .build();

        Struct structWithMapNested = new Struct(schemaWithMapNested)
            .put("map", Map.of("key", new Struct(schemaWithMapNested.field("map").schema().valueSchema())
                .put("key", "value")));


        Map<String, Object> toJsonMap = StructToJsonMap.toJsonMap(structWithMapNested);
        assertEquals(Map.of("key", Map.of("key", "value")), toJsonMap.get("map"));
        assertEquals("{\"map\":{\"key\":{\"key\":\"value\"}}}", new ObjectMapper().writeValueAsString(toJsonMap));
        assertEquals(Map.of("map", Map.of("key", Map.of("key", "value"))), toJsonMap);
    }

    @Test
    public void structWithMapWithList() throws JsonProcessingException {
        Schema schemaWithMapNested = SchemaBuilder.struct()
                                                  .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA,
                                                      SchemaBuilder.struct()
                                                                   .field("key",
                                                                       SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                                                                   .build()
                                                  ).build())
                                                  .build();

        Struct structWithMapNested = new Struct(schemaWithMapNested)
            .put("map", Map.of("key", new Struct(schemaWithMapNested.field("map").schema().valueSchema())
                .put("key", List.of("value"))));


        Map<String, Object> toJsonMap = StructToJsonMap.toJsonMap(structWithMapNested);
        assertEquals(Map.of("key", Map.of("key", List.of("value"))), toJsonMap.get("map"));
        assertEquals("{\"map\":{\"key\":{\"key\":[\"value\"]}}}", new ObjectMapper().writeValueAsString(toJsonMap));
        assertEquals(Map.of("map", Map.of("key", Map.of("key", List.of("value")))), toJsonMap);
    }

    @Test
    public void mapWithValue() throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");

        Map<String, Object> toJsonMap = StructToJsonMap.handleMap(map);
        assertEquals("value", toJsonMap.get("key"));
        assertEquals(Map.of("key", "value"), toJsonMap);
        assertEquals("{\"key\":\"value\"}", new ObjectMapper().writeValueAsString(toJsonMap));

        map = new HashMap<>();
        map.put("key", 10);
        toJsonMap = StructToJsonMap.handleMap(map);
        assertEquals(10, toJsonMap.get("key"));
        assertEquals(Map.of("key", 10), toJsonMap);
        assertEquals("{\"key\":10}", new ObjectMapper().writeValueAsString(toJsonMap));
    }

    @Test
    public void mapWithMap() throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        map.put("map", Map.of("key", Map.of("key", Map.of("key1", "value1"))));


        Map<String, Object> toJsonMap = StructToJsonMap.handleMap(map);
        assertEquals(Map.of("key", Map.of("key", Map.of("key1", "value1"))), toJsonMap.get("map"));
        assertEquals("{\"map\":{\"key\":{\"key\":{\"key1\":\"value1\"}}}}",
            new ObjectMapper().writeValueAsString(toJsonMap));
        assertEquals(Map.of("map", Map.of("key", Map.of("key", Map.of("key1", "value1")))), toJsonMap);
    }

    @Test
    public void mapWithStruct() throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        Struct struct = new Struct(SchemaBuilder.struct().field("key", Schema.STRING_SCHEMA).build())
            .put("key", "value");
        map.put("map", struct);


        Map<String, Object> toJsonMap = StructToJsonMap.handleMap(map);
        assertEquals(Map.of("key", "value"), toJsonMap.get("map"));
        assertEquals("{\"map\":{\"key\":\"value\"}}", new ObjectMapper().writeValueAsString(toJsonMap));
        assertEquals(Map.of("map", Map.of("key", "value")), toJsonMap);
    }

    @Test
    public void mapWithStructWithMap() throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        Schema schemaWithMapNested = SchemaBuilder.struct()
                                                  .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA,
                                                                                 Schema.STRING_SCHEMA)
                                                                             .build()
                                                  ).build();

        Struct structWithMapNested = new Struct(schemaWithMapNested)
            .put("map", Map.of("key", "value"));
        map.put("struct", structWithMapNested);


        Map<String, Object> toJsonMap = StructToJsonMap.handleMap(map);
        assertEquals(Map.of("map", Map.of("key", "value")), toJsonMap.get("struct"));
        assertEquals("{\"struct\":{\"map\":{\"key\":\"value\"}}}", new ObjectMapper().writeValueAsString(toJsonMap));
        assertEquals(Map.of("struct", Map.of("map", Map.of("key", "value"))), toJsonMap);
    }

    @Test
    public void mapWithList() throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        map.put("map", List.of("value1", "value2"));


        Map<String, Object> toJsonMap = StructToJsonMap.handleMap(map);
        assertEquals(List.of("value1", "value2"), toJsonMap.get("map"));
        assertEquals("{\"map\":[\"value1\",\"value2\"]}", new ObjectMapper().writeValueAsString(toJsonMap));
        assertEquals(Map.of("map", List.of("value1", "value2")), toJsonMap);
    }

    @Test
    public void mapWithListWithMap() throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        map.put("map", List.of(Map.of("key", "value1"), Map.of("key", "value2")));


        Map<String, Object> toJsonMap = StructToJsonMap.handleMap(map);
        assertEquals(List.of(Map.of("key", "value1"), Map.of("key", "value2")), toJsonMap.get("map"));
        assertEquals("{\"map\":[{\"key\":\"value1\"},{\"key\":\"value2\"}]}",
            new ObjectMapper().writeValueAsString(toJsonMap));
        assertEquals(Map.of("map", List.of(Map.of("key", "value1"), Map.of("key", "value2"))), toJsonMap);
    }

    @Test
    public void mapWithListWithStruct() throws JsonProcessingException {
        Map<String, Object> map = new HashMap<>();
        Struct struct1 = new Struct(SchemaBuilder.struct().field("key1", Schema.STRING_SCHEMA).build())
            .put("key1", "value1");
        Struct struct2 = new Struct(SchemaBuilder.struct().field("key2", Schema.STRING_SCHEMA).build())
            .put("key2", "value2");
        map.put("map", List.of(struct1, struct2));


        Map<String, Object> toJsonMap = StructToJsonMap.handleMap(map);
        assertEquals(List.of(Map.of("key1", "value1"), Map.of("key2", "value2")), toJsonMap.get("map"));
        assertEquals("{\"map\":[{\"key1\":\"value1\"},{\"key2\":\"value2\"}]}",
            new ObjectMapper().writeValueAsString(toJsonMap));
        assertEquals(Map.of("map", List.of(Map.of("key1", "value1"), Map.of("key2", "value2"))), toJsonMap);
    }

}

// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink.id.strategy;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(Parameterized.class)
public class ProvidedInStrategyTest {
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> parameters() {
        return List.of(
                new Object[]{ProvidedInValueStrategy.class, new ProvidedInValueStrategy()},
                new Object[]{ProvidedInKeyStrategy.class, new ProvidedInKeyStrategy()}
        );
    }

    @Parameterized.Parameter(0)
    public Class<?> clazz;

    @Parameterized.Parameter(1)
    public IdStrategy strategy;

    @Mock
    SinkRecord record;

    @Before
    public void setUp() {
        initMocks(this);

        strategy.configure(Map.of());
    }

    private void returnOnKeyOrValue(Schema schema, Object ret) {
        if (clazz == ProvidedInKeyStrategy.class) {
            when(record.keySchema()).thenReturn(schema);
            when(record.key()).thenReturn(ret);
        } else {
            when(record.valueSchema()).thenReturn(schema);
            when(record.value()).thenReturn(ret);
        }
    }

    @Test(expected = ConnectException.class)
    public void valueNotStructOrMapShouldFail() {
        returnOnKeyOrValue(Schema.STRING_SCHEMA, "a string");
        strategy.generateId(record);
    }

    @Test(expected = ConnectException.class)
    public void noIdInValueShouldFail() {
        returnOnKeyOrValue(null, Map.of());
        strategy.generateId(record);
    }

    @Test
    public void stringIdOnMapShouldReturn() {
        returnOnKeyOrValue(null, Map.of(
                "id", "1234567"
        ));
        assertEquals("1234567", strategy.generateId(record));
    }

    @Test
    public void nonStringIdOnMapShouldReturn() {
        returnOnKeyOrValue(null, Map.of(
                "id", 1234567
        ));
        assertEquals("1234567", strategy.generateId(record));
    }

    @Test
    public void stringIdOnStructShouldReturn() {
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();
        Struct struct = new Struct(schema)
                .put("id", "1234567");
        returnOnKeyOrValue(struct.schema(), struct);

        assertEquals("1234567", strategy.generateId(record));
    }

    @Test
    public void structIdOnStructShouldReturn() {
        Schema idSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("id", idSchema)
                .build();
        Struct struct = new Struct(schema)
                .put("id", new Struct(idSchema).put("name", "cosmos kramer"));
        returnOnKeyOrValue(struct.schema(), struct);

        assertEquals("{\"name\":\"cosmos kramer\"}", strategy.generateId(record));
    }

    @Test
    public void jsonPathOnStruct() {
        strategy.configure(Map.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id.name"));

        Schema idSchema = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .build();
        Schema schema = SchemaBuilder.struct()
            .field("id", idSchema)
            .build();
        Struct struct = new Struct(schema)
            .put("id", new Struct(idSchema).put("name", "franz kafka"));
        returnOnKeyOrValue(struct.schema(), struct);

        assertEquals("franz kafka", strategy.generateId(record));
    }

    @Test
    public void jsonPathOnMap() {
        strategy.configure(Map.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id.name"));
        returnOnKeyOrValue(null,
            Map.of("id", Map.of("name", "franz kafka")));

        assertEquals("franz kafka", strategy.generateId(record));
    }

    @Test(expected = ConnectException.class)
    public void invalidJsonPathThrows() {
        strategy.configure(Map.of(ProvidedInConfig.JSON_PATH_CONFIG, "invalid.path"));
        returnOnKeyOrValue(null,
            Map.of("id", Map.of("name", "franz kafka")));

        strategy.generateId(record);
    }

    @Test(expected = ConnectException.class)
    public void jsonPathNotExistThrows() {
        strategy.configure(Map.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id.not.exist"));
        returnOnKeyOrValue(null,
            Map.of("id", Map.of("name", "franz kafka")));

        strategy.generateId(record);
    }

    @Test
    public void complexJsonPath() {
        Map<String, Object> map1 = new LinkedHashMap<>();
        map1.put("id", 0);
        map1.put("name", "cosmos kramer");
        map1.put("occupation", "unknown");
        Map<String, Object> map2 = new LinkedHashMap<>();
        map2.put("id", 1);
        map2.put("name", "franz kafka");
        map2.put("occupation", "writer");
        returnOnKeyOrValue(null, Map.of("id", List.of(map1, map2)));

        strategy.configure(Map.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id[0].name"));
        assertEquals("cosmos kramer", strategy.generateId(record));

        strategy.configure(Map.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id[1].name"));
        assertEquals("franz kafka", strategy.generateId(record));

        strategy.configure(Map.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id[*].id"));
        assertEquals("[0,1]", strategy.generateId(record));

        strategy.configure(Map.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id"));
        assertEquals(
            "[{\"id\":0,\"name\":\"cosmos kramer\",\"occupation\":\"unknown\"},{\"id\":1,\"name\":\"franz kafka\",\"occupation\":\"writer\"}]",
            strategy.generateId(record));
    }

    @Test
    public void generatedIdSanitized() {
        returnOnKeyOrValue(null, Map.of("id", "#my/special\\id?"));

        String id = strategy.generateId(record);
        assertEquals("_my_special_id_", id);
    }
}

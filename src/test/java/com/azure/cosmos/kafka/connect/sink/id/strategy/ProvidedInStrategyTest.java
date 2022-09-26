// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink.id.strategy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(Parameterized.class)
public class ProvidedInStrategyTest {
    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> parameters() {
        return ImmutableList.of(
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

        strategy.configure(ImmutableMap.of());
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
        returnOnKeyOrValue(null, ImmutableMap.of());
        strategy.generateId(record);
    }

    @Test
    public void stringIdOnMapShouldReturn() {
        returnOnKeyOrValue(null, ImmutableMap.of(
                "id", "1234567"
        ));
        assertEquals("1234567", strategy.generateId(record));
    }

    @Test
    public void nonStringIdOnMapShouldReturn() {
        returnOnKeyOrValue(null, ImmutableMap.of(
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
        strategy.configure(ImmutableMap.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id.name"));

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
        strategy.configure(ImmutableMap.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id.name"));
        returnOnKeyOrValue(null,
            ImmutableMap.of("id", ImmutableMap.of("name", "franz kafka")));

        assertEquals("franz kafka", strategy.generateId(record));
    }

    @Test(expected = ConnectException.class)
    public void invalidJsonPathThrows() {
        strategy.configure(ImmutableMap.of(ProvidedInConfig.JSON_PATH_CONFIG, "invalid.path"));
        returnOnKeyOrValue(null,
            ImmutableMap.of("id", ImmutableMap.of("name", "franz kafka")));

        strategy.generateId(record);
    }

    @Test(expected = ConnectException.class)
    public void jsonPathNotExistThrows() {
        strategy.configure(ImmutableMap.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id.not.exist"));
        returnOnKeyOrValue(null,
            ImmutableMap.of("id", ImmutableMap.of("name", "franz kafka")));

        strategy.generateId(record);
    }

    @Test
    public void complexJsonPath() {
        returnOnKeyOrValue(null,
            ImmutableMap.of("id", ImmutableList.of(
                ImmutableMap.of("id", 0,
                    "name", "cosmos kramer",
                    "occupation", "unknown"),
                ImmutableMap.of("id", 1,
                    "name", "franz kafka",
                    "occupation", "writer")
            )));

        strategy.configure(ImmutableMap.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id[0].name"));
        assertEquals("cosmos kramer", strategy.generateId(record));

        strategy.configure(ImmutableMap.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id[1].name"));
        assertEquals("franz kafka", strategy.generateId(record));

        strategy.configure(ImmutableMap.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id[*].id"));
        assertEquals("[0,1]", strategy.generateId(record));

        strategy.configure(ImmutableMap.of(ProvidedInConfig.JSON_PATH_CONFIG, "$.id"));
        assertEquals(
            "[{\"id\":0,\"name\":\"cosmos kramer\",\"occupation\":\"unknown\"},{\"id\":1,\"name\":\"franz kafka\",\"occupation\":\"writer\"}]",
            strategy.generateId(record));
    }

    @Test
    public void generatedIdSanitized() {
        returnOnKeyOrValue(null, ImmutableMap.of("id", "#my/special\\id?"));

        String id = strategy.generateId(record);
        assertEquals("_my_special_id_", id);
    }
}

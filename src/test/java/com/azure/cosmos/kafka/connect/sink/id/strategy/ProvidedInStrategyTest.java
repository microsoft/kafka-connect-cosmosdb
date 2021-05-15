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
    }

    private void returnOnKeyOrValue(Object ret) {
        if (clazz == ProvidedInKeyStrategy.class) {
            when(record.key()).thenReturn(ret);
        } else {
            when(record.value()).thenReturn(ret);
        }
    }

    @Test(expected = ConnectException.class)
    public void valueNotStructOrMapShouldFail() {
        returnOnKeyOrValue("a string");
        strategy.generateId(record);
    }

    @Test(expected = ConnectException.class)
    public void noIdInValueShouldFail() {
        returnOnKeyOrValue(ImmutableMap.of());
        strategy.generateId(record);
    }

    @Test
    public void stringIdOnMapShouldReturn() {
        returnOnKeyOrValue(ImmutableMap.of(
                "id", "1234567"
        ));
        assertEquals("1234567", strategy.generateId(record));
    }

    @Test
    public void nonStringIdOnMapShouldReturn() {
        returnOnKeyOrValue(ImmutableMap.of(
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
        returnOnKeyOrValue(struct);

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
        returnOnKeyOrValue(struct);

        assertEquals("{\"name\":\"cosmos kramer\"}", strategy.generateId(record));
    }
}

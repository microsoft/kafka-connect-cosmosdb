// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.kafka.connect.sink.id.strategy;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TemplateStrategyTest {
    IdStrategy strategy = new TemplateStrategy();

    @Test
    public void simpleKey() {
        strategy.configure(Map.of(TemplateStrategyConfig.TEMPLATE_CONFIG, "${key}"));
        SinkRecord record = mock(SinkRecord.class);
        when(record.keySchema()).thenReturn(Schema.STRING_SCHEMA);
        when(record.key()).thenReturn("test");

        String id = strategy.generateId(record);
        assertEquals("test", id);
    }

    @Test
    public void kafkaMetadata() {
        strategy.configure(Map.of(TemplateStrategyConfig.TEMPLATE_CONFIG, "${topic}-${partition}-${offset}"));
        SinkRecord record = mock(SinkRecord.class);
        when(record.topic()).thenReturn("mytopic");
        when(record.kafkaPartition()).thenReturn(0);
        when(record.kafkaOffset()).thenReturn(1L);

        String id = strategy.generateId(record);
        assertEquals("mytopic-0-1", id);
    }

    @Test
    public void unknownVariablePreserved() {
        strategy.configure(Map.of(TemplateStrategyConfig.TEMPLATE_CONFIG, "${unknown}"));
        String id = strategy.generateId(mock(SinkRecord.class));
        assertEquals("${unknown}", id);
    }

    @Test
    public void nestedStruct() {
        strategy.configure(Map.of(TemplateStrategyConfig.TEMPLATE_CONFIG, "${key}"));
        SinkRecord record = mock(SinkRecord.class);
        Schema nestedSchema = SchemaBuilder.struct()
                .field("nested_field", Schema.STRING_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("string_field", Schema.STRING_SCHEMA)
                .field("struct_field", nestedSchema)
                .build();
        Struct value = new Struct(schema)
                .put("string_field", "value")
                .put("struct_field",
                        new Struct(nestedSchema).put("nested_field", "a nest"));
        when(record.keySchema()).thenReturn(schema);
        when(record.key()).thenReturn(value);

        String id = strategy.generateId(record);
        assertEquals(
                "{\"string_field\":\"value\",\"struct_field\":{\"nested_field\":\"a nest\"}}",
                id);
    }

    @Test
    public void fullKeyStrategyUsesFullKey() {
        strategy = new FullKeyStrategy();
        strategy.configure(Map.of());
        SinkRecord record = mock(SinkRecord.class);
        Schema schema = SchemaBuilder.struct()
                .field("string_field", Schema.STRING_SCHEMA)
                .field("int64_field", Schema.INT64_SCHEMA)
                .build();
        Struct value = new Struct(schema)
                .put("string_field", "value")
                .put("int64_field", 0L);

        when(record.keySchema()).thenReturn(schema);
        when(record.key()).thenReturn(value);

        String id = strategy.generateId(record);
        assertEquals(
                "{\"string_field\":\"value\",\"int64_field\":0}",
                id);
    }

    @Test
    public void metadataStrategyUsesMetadataWithDeliminator() {
        strategy = new KafkaMetadataStrategy();
        strategy.configure(Map.of(KafkaMetadataStrategyConfig.DELIMITER_CONFIG, "_"));
        SinkRecord record = mock(SinkRecord.class);
        when(record.topic()).thenReturn("topic");
        when(record.kafkaPartition()).thenReturn(0);
        when(record.kafkaOffset()).thenReturn(1L);

        String id = strategy.generateId(record);
        assertEquals("topic_0_1", id);
    }

    @Test
    public void generatedIdSanitized() {
        strategy = new TemplateStrategy();
        strategy.configure(
            Map.of(TemplateStrategyConfig.TEMPLATE_CONFIG, "#my/special\\id?"));
        SinkRecord record = mock(SinkRecord.class);

        String id = strategy.generateId(record);
        assertEquals("_my_special_id_", id);
    }
}

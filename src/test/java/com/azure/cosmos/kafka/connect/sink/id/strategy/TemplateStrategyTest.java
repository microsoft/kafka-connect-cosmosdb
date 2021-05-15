package com.azure.cosmos.kafka.connect.sink.id.strategy;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TemplateStrategyTest {
    IdStrategy strategy = new TemplateStrategy();

    @Test
    public void simpleKey() {
        strategy.configure(ImmutableMap.of(TemplateStrategyConfig.TEMPLATE_CONFIG, "${key}"));
        SinkRecord record = mock(SinkRecord.class);
        when(record.keySchema()).thenReturn(Schema.STRING_SCHEMA);
        when(record.key()).thenReturn("test");

        String id = strategy.generateId(record);
        assertEquals("test", id);
    }

    @Test
    public void simpleValue() {
        strategy.configure(ImmutableMap.of(TemplateStrategyConfig.TEMPLATE_CONFIG, "${value}"));
        SinkRecord record = mock(SinkRecord.class);
        when(record.valueSchema()).thenReturn(Schema.INT64_SCHEMA);
        when(record.value()).thenReturn(1024);

        String id = strategy.generateId(record);
        assertEquals("1024", id);
    }

    @Test
    public void kafkaMetadata() {
        strategy.configure(ImmutableMap.of(TemplateStrategyConfig.TEMPLATE_CONFIG, "${topic}-${partition}-${offset}"));
        SinkRecord record = mock(SinkRecord.class);
        when(record.topic()).thenReturn("mytopic");
        when(record.kafkaPartition()).thenReturn(0);
        when(record.kafkaOffset()).thenReturn(1L);

        String id = strategy.generateId(record);
        assertEquals("mytopic-0-1", id);
    }

    @Test
    public void complexKeyValue() {
        strategy.configure(ImmutableMap.of(TemplateStrategyConfig.TEMPLATE_CONFIG, "${key}-${value}"));
        SinkRecord record = mock(SinkRecord.class);
        Schema schema = SchemaBuilder.struct()
                .field("string_field", Schema.STRING_SCHEMA)
                .field("int_field", Schema.INT32_SCHEMA)
                .build();
        Struct key = new Struct(schema)
                .put("string_field", "key")
                .put("int_field", 0);
        Struct value = new Struct(schema)
                .put("string_field", "value")
                .put("int_field", 1);
        when(record.keySchema()).thenReturn(schema);
        when(record.valueSchema()).thenReturn(schema);
        when(record.key()).thenReturn(key);
        when(record.value()).thenReturn(value);

        String id = strategy.generateId(record);
        assertEquals(
                "{\"string_field\":\"key\",\"int_field\":0}-{\"string_field\":\"value\",\"int_field\":1}",
                id);
    }

    @Test
    public void unknownVariablePreserved() {
        strategy.configure(ImmutableMap.of(TemplateStrategyConfig.TEMPLATE_CONFIG, "${unknown}"));
        String id = strategy.generateId(mock(SinkRecord.class));
        assertEquals("${unknown}", id);
    }

    @Test
    public void nestedStruct() {
        strategy.configure(ImmutableMap.of(TemplateStrategyConfig.TEMPLATE_CONFIG, "${value}"));
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
        when(record.valueSchema()).thenReturn(schema);
        when(record.value()).thenReturn(value);

        String id = strategy.generateId(record);
        assertEquals(
                "{\"string_field\":\"value\",\"struct_field\":{\"nested_field\":\"a nest\"}}",
                id);
    }

    @Test
    public void fullValueStrategyUsesFullValue() {
        strategy = new FullValueStrategy();
        strategy.configure(ImmutableMap.of());
        SinkRecord record = mock(SinkRecord.class);
        Schema schema = SchemaBuilder.struct()
                .field("string_field", Schema.STRING_SCHEMA)
                .field("int64_field", Schema.INT64_SCHEMA)
                .build();
        Struct value = new Struct(schema)
                .put("string_field", "value")
                .put("int64_field", 0L);

        when(record.valueSchema()).thenReturn(schema);
        when(record.value()).thenReturn(value);

        String id = strategy.generateId(record);
        assertEquals(
                "{\"string_field\":\"value\",\"int64_field\":0}",
                id);
    }

    @Test
    public void fullKeyStrategyUsesFullKey() {
        strategy = new FullKeyStrategy();
        strategy.configure(ImmutableMap.of());
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
        strategy.configure(ImmutableMap.of(KafkaMetadataStrategyConfig.DELIMITER_CONFIG, "_"));
        SinkRecord record = mock(SinkRecord.class);
        when(record.topic()).thenReturn("topic");
        when(record.kafkaPartition()).thenReturn(0);
        when(record.kafkaOffset()).thenReturn(1L);

        String id = strategy.generateId(record);
        assertEquals("topic_0_1", id);
    }
}

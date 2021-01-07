package com.microsoft.azure.cosmosdb.kafka.connect.source;

import com.fasterxml.jackson.databind.JsonNode;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.connect.data.Values.convertToByte;
import static org.apache.kafka.connect.data.Values.convertToDouble;
import static org.apache.kafka.connect.data.Values.convertToFloat;
import static org.apache.kafka.connect.data.Values.convertToInteger;
import static org.apache.kafka.connect.data.Values.convertToLong;
import static org.apache.kafka.connect.data.Values.convertToShort;

import static java.lang.String.format;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class JsonToStruct {
    private static final Logger logger = LoggerFactory.getLogger(CosmosDBSourceTask.class);
    public static final String SCHEMA_NAME_TEMPLATE = "inferred_name_%s";

    public SchemaAndValue recordToSchemaAndValue(final JsonNode node) {
        Schema nodeSchema = inferSchema(node);
        Struct struct = new Struct(nodeSchema);

        nodeSchema.fields().forEach(field -> {
            JsonNode fieldValue = node.get(field.name());
            if (fieldValue != null) {
                SchemaAndValue schemaAndValue = toSchemaAndValue(field.schema(), fieldValue);
                struct.put(field, schemaAndValue.value());
            } else {
                boolean optionalField = field.schema().isOptional();
                Object defaultValue = field.schema().defaultValue();
                if (optionalField || defaultValue != null) {
                    struct.put(field, defaultValue);
                } else {
                    logger.error("Missing value for field {}", field.name());
                }
            }
        });
        return new SchemaAndValue(nodeSchema, struct);
    }

    private Schema inferSchema(JsonNode jsonValue) {
        switch (jsonValue.getNodeType()) {
            case NULL:
                return Schema.OPTIONAL_STRING_SCHEMA;
            case BOOLEAN:
                return Schema.BOOLEAN_SCHEMA;
            case NUMBER:
                if (jsonValue.isIntegralNumber()) {
                    return Schema.INT64_SCHEMA;
                } else {
                    return Schema.FLOAT64_SCHEMA;
                }
            case ARRAY:
                SchemaBuilder arrayBuilder = SchemaBuilder
                        .array(jsonValue.elements().hasNext() ? inferSchema(jsonValue.elements().next())
                                : Schema.OPTIONAL_STRING_SCHEMA);
                arrayBuilder.name(generateName(arrayBuilder));
                return arrayBuilder.build();
            case OBJECT:
                SchemaBuilder structBuilder = SchemaBuilder.struct();
                Iterator<Map.Entry<String, JsonNode>> it = jsonValue.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> entry = it.next();
                    structBuilder.field(entry.getKey(), inferSchema(entry.getValue()));
                }
                structBuilder.name(generateName(structBuilder));
                return structBuilder.build();
            case STRING:
                return Schema.STRING_SCHEMA;
            case BINARY:
            case MISSING:
            case POJO:
            default:
                return null;
        }
    }

    // Generate Unique Schema Name
    public static String generateName(final SchemaBuilder builder) {
        return format(SCHEMA_NAME_TEMPLATE, Objects.hashCode(builder.build())).replace("-", "_");
    }

    private SchemaAndValue toSchemaAndValue(final Schema schema, final JsonNode node) {
        SchemaAndValue schemaAndValue = new SchemaAndValue(schema, node);
        if (schema.isOptional() && node.isNull()) {
            return new SchemaAndValue(schema, null);
        }
        switch (schema.type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
                schemaAndValue = numberToSchemaAndValue(schema, node);
                break;
            case BOOLEAN:
                schemaAndValue = new SchemaAndValue(schema, node.asBoolean());
                break;
            case STRING:
                schemaAndValue = new SchemaAndValue(schema, node.asText());
                break;
            case BYTES:
                schemaAndValue = new SchemaAndValue(schema, node);
                break;
            case ARRAY:
                schemaAndValue = new SchemaAndValue(schema, node);
                break;
            case MAP:
                schemaAndValue = new SchemaAndValue(schema, node);
                break;
            case STRUCT:
                schemaAndValue = recordToSchemaAndValue(node);
                break;
            default:
                logger.error("Unsupported Schema type: %s", schema.type());
        }
        return schemaAndValue;
    }

    private SchemaAndValue numberToSchemaAndValue(final Schema schema, final JsonNode nodeValue) {
        Object value = null;
        if (nodeValue.isNumber()) {
            if (nodeValue.isInt()) {
                value = nodeValue.intValue();
            } else if (nodeValue.isDouble()) {
                value = nodeValue.doubleValue();
            }
        } else {
            logger.error("Unexpted value %s for Schma %s", nodeValue, schema.type());
        }

        switch (schema.type()) {
            case INT8:
                value = convertToByte(schema, value);
                break;
            case INT16:
                value = convertToShort(schema, value);
                break;
            case INT32:
                value = convertToInteger(schema, value);
                break;
            case INT64:
                value = convertToLong(schema, value);
                break;
            case FLOAT32:
                value = convertToFloat(schema, value);
                break;
            case FLOAT64:
                value = convertToDouble(schema, value);
                break;
            default:
                logger.error("Unsupported Schema type: %s", schema.type());
        }
        return new SchemaAndValue(schema, value);
    }

}

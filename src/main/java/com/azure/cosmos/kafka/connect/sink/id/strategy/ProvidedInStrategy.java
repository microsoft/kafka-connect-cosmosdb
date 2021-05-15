package com.azure.cosmos.kafka.connect.sink.id.strategy;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

class ProvidedInStrategy extends AbstractIdStrategy {
    protected enum ProvidedIn {
        KEY,
        VALUE
    }

    private final ProvidedIn where;

    ProvidedInStrategy(ProvidedIn where) {
        this.where = where;
    }

    @Override
    public String generateId(SinkRecord record) {
        Object value = where == ProvidedIn.KEY ? record.key() : record.value();
        Schema idSchema = null;
        Object idValue;

        if (value instanceof Struct) {
            idSchema = ((Struct) value).schema().field(AbstractIdStrategyConfig.ID).schema();
            idValue = ((Struct) value).get(AbstractIdStrategyConfig.ID);
        } else if (value instanceof Map) {
            idValue = ((Map) value).get(AbstractIdStrategyConfig.ID);
        } else {
            throw new ConnectException("Expected " + where + " to be of struct or map type to use ProvidedIn strategy");
        }

        if (idValue == null) {
            throw new ConnectException("Cannot find id in " + where + ": " + value);
        }
        return Values.convertToString(idSchema, idValue);
    }
}

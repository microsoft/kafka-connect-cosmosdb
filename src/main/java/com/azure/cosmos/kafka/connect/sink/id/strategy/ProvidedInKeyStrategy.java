package com.azure.cosmos.kafka.connect.sink.id.strategy;

public class ProvidedInKeyStrategy extends ProvidedInStrategy {
    public ProvidedInKeyStrategy() {
        super(ProvidedIn.KEY);
    }
}

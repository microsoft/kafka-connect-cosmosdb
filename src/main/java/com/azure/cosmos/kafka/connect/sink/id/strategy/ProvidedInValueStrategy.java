package com.azure.cosmos.kafka.connect.sink.id.strategy;

public class ProvidedInValueStrategy extends ProvidedInStrategy {
    public ProvidedInValueStrategy() {
        super(ProvidedIn.VALUE);
    }
}

package com.microsoft.azure.cosmosdb.kafka.connect;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Maps Kafka topics to CosmosDB Collections
 */
public class TopicCollectionMap {
    private final BidiMap<String, String> map;

    private TopicCollectionMap(BidiMap<String, String> map) {
        this.map = map;
    }

    public static TopicCollectionMap deserialize(String input) {
        if (StringUtils.isEmpty(input)){
            return TopicCollectionMap.empty();
        }

        if (StringUtils.contains(input, '#')) { // There's at least one pair
            String[] items = StringUtils.split(input, ',');
            Stream<String[]> keyValuePairs = Arrays.stream(items).map(item -> {
                String[] pair = StringUtils.split(item, '#');
                pair[0] = StringUtils.trimToNull(pair[0]);
                pair[1] = StringUtils.trimToNull(pair[1]);
                return pair;
            });

            BidiMap<String, String> map = new DualHashBidiMap<>();
            keyValuePairs.forEach(pair -> map.put(pair[0], pair[1]));
            return new TopicCollectionMap(map);
        } else throw new IllegalArgumentException("Invalid topic collection map.");
    }

    public String serialize() {
        return map.entrySet().stream()
                .map(entry -> entry.getKey() + "#" + entry.getValue())
                .collect(Collectors.joining(","));
    }

    /**
     * Creates an empty map of topic collections
     *
     * @return
     */
    public static TopicCollectionMap empty() {
        return new TopicCollectionMap(new DualHashBidiMap<>());
    }


    //TODO: eliminate this mutability

    /**
     * Adds topic names, generating default collection names
     *
     * @param topicNames
     */
    public void addTopics(Collection<String> topicNames) {
        Objects.requireNonNull(topicNames);
        for (String topicName : topicNames) {
            map.putIfAbsent(topicName, topicName);
        }
    }

    public Optional<String> getCollectionForTopic(String topicName) {
        return Optional.ofNullable(map.get(topicName));
    }

    public Optional<String> getTopicForCollection(String collectionName) {
        return Optional.ofNullable(map.inverseBidiMap().get(collectionName));
    }

}



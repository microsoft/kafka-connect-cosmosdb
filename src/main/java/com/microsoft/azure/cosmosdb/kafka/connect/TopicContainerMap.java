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
 * Maps Kafka topics to CosmosDB Containers
 */
public class TopicContainerMap {


    private final BidiMap<String, String> map;

    private TopicContainerMap(BidiMap<String, String> map) {
        this.map = map;
    }

    public static TopicContainerMap deserialize(String input) {
        if (StringUtils.isEmpty(input)) {
            return TopicContainerMap.empty();
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
            return new TopicContainerMap(map);
        } else throw new IllegalArgumentException("Invalid topic container map.");
    }

    /**
     * Creates an empty map of topic containers
     *
     * @return Returns a map of Topics assigned to containers.
     */
    public static TopicContainerMap empty() {
        return new TopicContainerMap(new DualHashBidiMap<>());
    }

    public String serialize() {
        return map.entrySet().stream()
                .map(entry -> entry.getKey() + "#" + entry.getValue())
                .collect(Collectors.joining(","));
    }

    /**
     * Adds topic names, generating default container names
     *
     * @param topicNames Collection of topic names
     */
    public void addTopics(Collection<String> topicNames) {
        Objects.requireNonNull(topicNames);
        for (String topicName : topicNames) {
            map.putIfAbsent(topicName, topicName);
        }
    }

    public Optional<String> getContainerForTopic(String topicName) {
        return Optional.ofNullable(map.get(topicName));
    }

    public Optional<String> getTopicForContainer(String containerName) {
        return Optional.ofNullable(map.inverseBidiMap().get(containerName));
    }

}



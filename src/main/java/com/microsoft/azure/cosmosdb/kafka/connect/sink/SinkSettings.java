package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import com.microsoft.azure.cosmosdb.kafka.connect.Setting;
import com.microsoft.azure.cosmosdb.kafka.connect.Settings;
import org.apache.commons.collections4.ListUtils;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Arrays;
import java.util.List;

/**
 * Contains settings for the Kafka ComsosDB Sink Connector
 */
public class SinkSettings extends Settings {
    private String postProcessor;
    private final List<Setting> sinkSettings = Arrays.asList(
            //Add all sink settings here:
            new Setting(SinkTask.TOPICS_CONFIG, "List of topics to consume, separated by commas.", "Topics", s -> {
            }, () -> ""),
            new Setting(SinkTask.TOPICS_REGEX_CONFIG,
                    "Regular expression giving topics to consume. " +
                            "Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. " +
                            "Only one of " + SinkTask.TOPICS_CONFIG + " or " + SinkTask.TOPICS_REGEX_CONFIG + " should be specified.",
                    "Topics RegEx", s -> {
            }, () -> "")
    );

    @Override
    protected List<Setting> getAllSettings() {
        return ListUtils.union(super.getAllSettings(), sinkSettings);
    }
}

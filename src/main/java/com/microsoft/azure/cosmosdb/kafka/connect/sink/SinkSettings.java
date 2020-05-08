package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import com.microsoft.azure.cosmosdb.kafka.connect.Setting;
import com.microsoft.azure.cosmosdb.kafka.connect.Settings;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.Functions;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Contains settings for the Kafka ComsosDB Sink Connector
 */
public class SinkSettings extends Settings {
    private final List<Setting> sinkSettings = Arrays.asList(
            //Add all settings here:
            new Setting(Settings.PREFIX + ".sink.post-processor", "Comma-separated list of Sink Post-Processor class names to use for post-processing",
                    "Sink post-processor", this::setPostProcessor, this::getPostProcessor),

            new Setting(SinkTask.TOPICS_CONFIG, "List of topics to consume, separated by commas", "Topics", s -> {}, ()->""),
            new Setting(SinkTask.TOPICS_REGEX_CONFIG,
                    "Regular expression giving topics to consume. " +
                            "Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. " +
                            "Only one of " + SinkTask.TOPICS_CONFIG + " or " + SinkTask.TOPICS_REGEX_CONFIG + " should be specified.",
                    "Topics RegEx",".*", s -> {}, ()->"")
    );

    @Override
    protected List<Setting> getAllSettings() {
        return ListUtils.union(super.getAllSettings(), sinkSettings);
    }

    private String postProcessor;

    /**
     * Returns the sink post-processor list
     *
     * @Return The sink post-processor list
     */

    public String getPostProcessor() {
        return this.postProcessor;
    }


    /**
     * Sets the sink post-processor list
     *
     * @param postProcessor
     */
    public void setPostProcessor(String postProcessor) {
        this.postProcessor = postProcessor;
    }


}

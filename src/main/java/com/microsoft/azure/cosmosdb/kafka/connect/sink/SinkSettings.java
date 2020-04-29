package com.microsoft.azure.cosmosdb.kafka.connect.sink;

import com.microsoft.azure.cosmosdb.kafka.connect.Setting;
import com.microsoft.azure.cosmosdb.kafka.connect.Settings;
import org.apache.commons.collections4.ListUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Contains settings for the Kafka ComsosDB Sink Connector
 */
public class SinkSettings extends Settings {
    private final List<Setting> sinkSettings = Arrays.asList(
            //Add all settings here:
            new Setting(Settings.PREFIX + ".sink.post-processor", "Comma-separated list of Sink Post-Processor class names to use for post-processing",
                    "Sink post-processor", this::setPostProcessor, this::getPostProcessor)
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

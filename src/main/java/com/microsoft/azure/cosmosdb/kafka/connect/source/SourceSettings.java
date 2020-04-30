package com.microsoft.azure.cosmosdb.kafka.connect.source;

import com.microsoft.azure.cosmosdb.kafka.connect.Setting;
import com.microsoft.azure.cosmosdb.kafka.connect.Settings;
import org.apache.commons.collections4.ListUtils;

import java.util.Arrays;
import java.util.List;

/**
 * Contains settings for the CosmosDB Kafka Source Connector
 */
public class SourceSettings extends Settings {
    private final List<Setting> sourceSettings = Arrays.asList(
            new Setting(Settings.PREFIX + ".source.post-processor", "Comma-separated list of Source Post-Processor class names to use for post-processing",
                    "Sink post-processor", this::setPostProcessor, this::getPostProcessor),
            new Setting(Settings.PREFIX + ".assigned.partitions", "The CosmosDB partitions a task has been assigned",
                    "Assigned Partitions", this::setAssignedPartitions, this::getAssignedPartitions)

    );

    private String getAssignedPartitions() {
        return this.assignedPartitions;
    }

    private String assignedPartitions;

    private void setAssignedPartitions(String assignedPartitions) {
        this.assignedPartitions = assignedPartitions;
    }


    private String postProcessor;

    private String getPostProcessor() {
        return this.postProcessor;
    }

    private void setPostProcessor(String postProcessor) {
        this.postProcessor = postProcessor;
    }


    @Override
    protected List<Setting> getAllSettings() {
        return ListUtils.union(super.getAllSettings(), sourceSettings);
    }

}

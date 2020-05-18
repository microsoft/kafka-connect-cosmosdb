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
    private String assignedPartitions;
    private String postProcessor;
    private final List<Setting> sourceSettings = Arrays.asList(
            new Setting(Settings.PREFIX + ".source.post-processor", "Comma-separated list of Source Post-Processor class names to use for post-processing",
                    "Source post-processor", this::setPostProcessor, this::getPostProcessor),
            new Setting(Settings.PREFIX + ".assigned.container", "The CosmosDB Feed Container assigned to the task.",
                    "Assigned Container", this::setAssignedContainer, this::getAssignedContainer),
          new Setting(Settings.PREFIX + ".worker.name", "The CosmosDB worker name.",
            "Worker name", this::setWorkerName, this::getWorkerName)

    );


    private String workerName;

    public void setWorkerName(String workerName) {
        this.workerName = workerName;
    }

    public String getWorkerName() {
        return this.workerName;
    }

    private String assignedContainer;

    public String getAssignedContainer() {
        return this.assignedContainer;
    }

    public void setAssignedContainer(String assignedPartitions) {
        this.assignedContainer = assignedPartitions;
    }


    private String postProcessor;

    public String getPostProcessor() {
        return this.postProcessor;
    }

    public void setPostProcessor(String postProcessor) {
        this.postProcessor = postProcessor;
    }


    @Override
    protected List<Setting> getAllSettings() {
        return ListUtils.union(super.getAllSettings(), sourceSettings);
    }

}

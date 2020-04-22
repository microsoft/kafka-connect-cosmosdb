package com.microsoft.azure.cosmosdb.kafka.connect;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Settings {
    private static Logger logger = LoggerFactory.getLogger(Settings.class);

    public static final String PREFIX ="connect.cosmosdb";


    private final List<Setting> allSettings = Arrays.asList(
            //Add all settings here:
         new Setting(PREFIX +".task.buffer.size", this::setPollingInterval, this::getPollingInterval),
            new Setting(PREFIX+".task.timeout", this::setTaskTimeout, this::getTaskTimeout)
    );

    /**
     * Returns all the available settings
     * @return
     */
    protected List<Setting> getAllSettings(){
        return Collections.unmodifiableList(allSettings);
    }

    /**
     * Populates this object with all the settings values in a map
     * @param values Map of setting names to values
     */
    public void populate(Map<String, String> values){
        for (Setting setting : getAllSettings()){
            Optional<String> value = Optional.ofNullable(values.get(setting.getName()));
            value.ifPresent(setting.getModifier());
        }
    }

    private String pollingInterval;

    public String getPollingInterval(){
        return pollingInterval;
    }

    public void setPollingInterval(String pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    private String taskTimeout;

    public String getTaskTimeout() {
        return taskTimeout;
    }

    public void setTaskTimeout(String taskTimeout) {
        this.taskTimeout = taskTimeout;
    }
}


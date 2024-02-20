package com.span;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypesenseSinkConnector extends SinkConnector {
    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        // Initialize your connector here
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TypesenseSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        // Clean up resources
    }

    @Override
    public ConfigDef config() {
        // Define your connector's configuration properties here
        return new ConfigDef();
    }
}


package com.span;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypesenseSinkConnector extends SinkConnector {
    public static final String primaryKeyenabled = "primaryKeyenabled";
    static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(primaryKeyenabled, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Enable primary key for document ID generation");
    private Map<String, String> props;
    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        // Validate configuration
        this.props = props;
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        String primaryKeysEnabled = config.getString(primaryKeyenabled);
        System.out.println("The SINK CONNECTOR ParseBoolean");
        System.out.println(primaryKeysEnabled);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TypesenseSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(props);
        }
        return configs;
    }

    @Override
    public void stop() {
        // Clean up resources
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}

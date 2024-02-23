package com.span;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.typesense.api.*;
import org.typesense.model.*;
import org.typesense.resources.*;

import java.lang.Override;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.time.Duration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


public class TypesenseSinkTask extends SinkTask {
    private Client typesenseClient;
    private Map<String, Boolean> collectionCreatedMap = new HashMap<>();
    private String primaryKeysEnabled;
    @Override
    public String version() {
        return "1.0";
    }
    public TypesenseSinkTask(){

    }

    @Override
    public void start(Map<String, String> props) {
        // Initialize your Typesense client here
        AbstractConfig config = new AbstractConfig(TypesenseSinkConnector.CONFIG_DEF, props);
        primaryKeysEnabled = config.getString(TypesenseSinkConnector.primaryKeyenabled);
        List<Node> nodes = new ArrayList<>();
        nodes.add(new Node("http", "150.136.139.228", "8108"));
        Configuration configuration = new Configuration(nodes, Duration.ofSeconds(2), "xyz");
        typesenseClient = new Client(configuration);
        // Set the primary key enabled flag from the configuration
        System.out.println("The SINK TASK ParseBoolean");
        System.out.println(primaryKeysEnabled);
        System.out.println("The SINK TASK ParseBoolean");
        System.out.println(primaryKeysEnabled);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        // Iterate over the records and index them into Typesense
        final ObjectMapper objectMapper = new ObjectMapper();
        for (SinkRecord record : records) {
            try {
                String topic = record.topic();
                Map<String, Object> jsonMap = objectMapper.readValue(record.value().toString(), new TypeReference<Map<String, Object>>() {});

                // Convert all fields to their appropriate types
                convertFieldTypes(jsonMap, primaryKeysEnabled);
                System.out.println("The SINK TASK ParseBoolean in PUT");
                System.out.println(primaryKeysEnabled);
                // Create collection schema if not already created for this topic
                if (!collectionCreatedMap.containsKey(topic) || !collectionCreatedMap.get(topic)) {
                    createCollectionSchema(topic, jsonMap.keySet());
                    collectionCreatedMap.put(topic, true);
                }

                // Index the document into Typesense
                typesenseClient.collections(topic).documents().upsert(jsonMap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void stop() {
    }

    private void createCollectionSchema(String topic, Set<String> fields) {
        // Create fields list for Typesense collection schema
        List<Field> typesenseFields = new ArrayList<>();
        for (String field : fields) {
            typesenseFields.add(new Field().name(field).type(FieldTypes.STRING));
        }

        // Create collection schema
        CollectionSchema collectionSchema = new CollectionSchema()
                .name(topic)
                .fields(typesenseFields);

        // Create the collection in Typesense
        try {
            typesenseClient.collections().create(collectionSchema);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

   private void convertFieldTypes(Map<String, Object> jsonMap,String primaryKeyEnabled) {
    // Get the existing keys to avoid ConcurrentModificationException
    Set<String> keys = new HashSet<>(jsonMap.keySet());

    // Convert all fields to their appropriate types
    for (String key : keys) {
        Object value = jsonMap.get(key);
        // Convert non-string values to strings
        if (value == null) {
            jsonMap.put(key, "null");
        }
        if (value != null && !(value instanceof String)) {
            jsonMap.put(key, value.toString());
        }
    }
    System.out.println("The convertFieldTypes ParseBoolean: " + primaryKeyEnabled); // This line was adjusted
    if (primaryKeyEnabled.equalsIgnoreCase("true")) {
        String firstField = jsonMap.keySet().iterator().next();
        jsonMap.put("id", jsonMap.get(firstField));
    }
}
}

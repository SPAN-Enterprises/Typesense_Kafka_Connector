package com.span;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.typesense.api.*;
import org.typesense.model.*;
import org.typesense.resources.*;

import java.io.IOException;
import java.lang.Override;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.time.Duration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TypesenseSinkTask extends SinkTask {
    private Client typesenseClient;
    private Map<String, Boolean> collectionCreatedMap = new HashMap<>();
    private String primaryKeysEnabled;
    final ObjectMapper objectMapper = new ObjectMapper();
    private AbstractConfig config;

    @Override
    public String version() {
        return "1.0";
    }

    public TypesenseSinkTask() {

    }

    @Override
    public void start(Map<String, String> props) {
        // Initialize your Typesense client here
        config = new AbstractConfig(TypesenseSinkConnector.CONFIG_DEF, props);
        primaryKeysEnabled = config.getString(TypesenseSinkConnector.primaryKeyenabled);
        List<Node> nodes = new ArrayList<>();
        nodes.add(new Node("http", config.getString("Host"), config.getString("Port")));
        Configuration configuration = new Configuration(nodes, Duration.ofSeconds(2), config.getString("API_Key"));
        typesenseClient = new Client(configuration);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        // Iterate over the records and index them into Typesense
        for (SinkRecord record : records) {
            try {
                String topic = record.topic();
                boolean idSet = false;
                Map<String, Object> jsonMap = flattenJson(
                        objectMapper.readValue(record.value().toString(), new TypeReference<Map<String, Object>>() {
                        }), primaryKeysEnabled, idSet);
                // Convert all fields to their appropriate types
                convertFieldTypes(jsonMap, config.getString("Timestamps"),config.getString("Mask"),config.getInt("Mask Limit"));
                // Create collection schema if not already created for this topic
                if (!collectionCreatedMap.containsKey(topic) || !collectionCreatedMap.get(topic)) {
                    createCollectionSchema(topic, jsonMap.keySet());
                    collectionCreatedMap.put(topic, true);
                }
                // Check for missing fields in jsonMap and add them with default values or null
                CollectionSchema collectionSchema = typesenseClient.collections(topic).retrieve();
                for (Field field : collectionSchema.getFields()) {
                    if (!jsonMap.containsKey(field.getName())) {
                        // Add missing field to jsonMap with default value or null
                        jsonMap.put(field.getName(), "Null");
                    }
                }

                // Index the document into Typesense
                if (primaryKeysEnabled.equalsIgnoreCase("true")) {
                    typesenseClient.collections(topic).documents().upsert(jsonMap);
                } else {
                    typesenseClient.collections(topic).documents().create(jsonMap);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> flattenJson(Map<String, Object> jsonMap, String primaryKeyEnabled, boolean idSet) {
        Map<String, Object> flattenedMap = new LinkedHashMap<>();

        try {
            if (primaryKeyEnabled.equalsIgnoreCase("true") && !idSet) {
                for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
                    if (entry.getValue() != null) {
                        flattenedMap.put("id", entry.getValue().toString());
                        idSet = true; // Set the flag to true after setting the "id" field
                        break; // Break out of the loop after setting the "id" field
                    }
                }
            }
            for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (value instanceof String && ((String) value).startsWith("{") && ((String) value).endsWith("}")) {
                    // Parse the JSON string into a Map
                    Map<String, Object> nestedMap = objectMapper.readValue((String) value,
                            new TypeReference<Map<String, Object>>() {
                            });
                    // Flatten the nested JSON recursively
                    Map<String, Object> flattenedNestedMap = flattenJson(nestedMap, primaryKeyEnabled, idSet);
                    // Prefix keys with the original key and add to flattenedMap
                    for (Map.Entry<String, Object> nestedEntry : flattenedNestedMap.entrySet()) {
                        flattenedMap.put(key + "_" + nestedEntry.getKey(), nestedEntry.getValue());
                    }
                } else if (value instanceof Map) {
                    // Recursively flatten nested object
                    Map<String, Object> flattenedNestedMap = flattenJson((Map<String, Object>) value, primaryKeyEnabled,
                            idSet);
                    // Prefix keys with the original key and add to flattenedMap
                    for (Map.Entry<String, Object> nestedEntry : flattenedNestedMap.entrySet()) {
                        if (nestedEntry.getValue() != null) {
                            flattenedMap.put(key + "_" + nestedEntry.getKey(), nestedEntry.getValue());
                        } else {
                            flattenedMap.put(key + "_" + nestedEntry.getKey(), "Null");
                        }
                    }
                } else {
                    // Check if the field is not present in the document
                    if (!jsonMap.containsKey(key)) {
                        // Use default value or existing value
                        flattenedMap.put(key, null); // Use null as default value
                    } else {
                        if (value != null) {
                            flattenedMap.put(key, value);
                        }
                        else{
                            flattenedMap.put(key, null);
                        }
                    }
                }
            }
        } catch (NullPointerException e) {
            e.printStackTrace();
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return flattenedMap;
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
                .fields(typesenseFields).enableNestedFields(true);

        // Create the collection in Typesense
        try {
            typesenseClient.collections().create(collectionSchema);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void convertFieldTypes(Map<String, Object> jsonMap, String timestampsConfig,String MaskConfig, Integer maskchar) {
        Map<String, Object> convertedMap = new HashMap<>();
        Set<String> timestampFields = new HashSet<>(Arrays.asList(timestampsConfig.split(",")));
        Set<String> maskFields = new HashSet<>(Arrays.asList(MaskConfig.split(",")));
        for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            // Handle null values
            if (value == null) {
                convertedMap.put(key, "null");
                continue;
            }

            // Convert epoch time to date time for specified fields
            if (timestampFields.contains(key) && value instanceof Long) {
                // Convert epoch time to date time
                long epochTime = (Long) value;
                Date date = new Date(epochTime); // Convert milliseconds directly
                SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy, h:mm:ss a", Locale.ENGLISH);
                sdf.setTimeZone(TimeZone.getTimeZone("UTC")); // Set the timezone to UTC
                convertedMap.put(key, sdf.format(date));
            } else {
                if(maskFields.contains(key)){
                    String maskedValue = maskValue(value.toString(), maskchar, '*');
                    convertedMap.put(key, maskedValue);
                }
                else{
                    if (!(value instanceof String)) {
                        convertedMap.put(key, value.toString());
                    } else {
                        // Copy other fields as they are
                        convertedMap.put(key, value);
                    }
                }          
            }
        }
        // Update the original map
        jsonMap.clear();
        jsonMap.putAll(convertedMap);
    }

    private String maskValue(String value, int visibleChars, char maskChar) {
        if (value.length() <= visibleChars) {
            return value;
        }
    
        StringBuilder maskedValue = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            if (i < value.length() - visibleChars) {
                maskedValue.append(maskChar);
            } else {
                maskedValue.append(value.charAt(i));
            }
        }
    
        return maskedValue.toString();
    }
    

}

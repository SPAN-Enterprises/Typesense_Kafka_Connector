package com.span;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.typesense.api.*;
import org.typesense.model.*;
import org.typesense.resources.*;

import java.lang.Override;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.time.Duration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


public class TypesenseSinkTask extends SinkTask {
    private Client typesenseClient;
    private boolean collectionCreated = false;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        // Initialize your Typesense client here
        List<Node> nodes = new ArrayList<>();
        nodes.add(new Node("http", "150.136.139.228", "8108"));
        Configuration configuration = new Configuration(nodes, Duration.ofSeconds(2), "xyz");
        typesenseClient = new Client(configuration);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        // Create collection schema if not already created
        if (!collectionCreated) {
            createCollectionSchema(records);
            collectionCreated = true;
        }

        // Iterate over the records and index them into Typesense
        final ObjectMapper objectMapper = new ObjectMapper();
        for (SinkRecord record : records) {
            try {
                Map<String, Object> jsonMap = objectMapper.readValue(record.value().toString(), new TypeReference<Map<String, Object>>() {});
                
                // Index the document into Typesense
                typesenseClient.collections("Span_Test").documents().create(jsonMap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void stop() {
    }
    
    private void createCollectionSchema(Collection<SinkRecord> records) {
        final ObjectMapper objectMapper = new ObjectMapper();
        Set<String> fields = new HashSet<>();
        for (SinkRecord record : records) {
            try {
                Map<String, Object> jsonMap = objectMapper.readValue(record.value().toString(), new TypeReference<Map<String, Object>>() {});
                extractFields(jsonMap, "", fields);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Create fields list for Typesense collection schema
        List<Field> typesenseFields = new ArrayList<>();
        for (String field : fields) {
            typesenseFields.add(new Field().name(field).type(FieldTypes.STRING));
        }

        // Create collection schema
        CollectionSchema collectionSchema = new CollectionSchema()
                .name("Span_Test")
                .fields(typesenseFields);

        // Create the collection in Typesense
        try {
            typesenseClient.collections().create(collectionSchema);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    private void extractFields(Map<String, Object> jsonMap, String parent, Set<String> fields) {
        for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
            String fieldName = parent.isEmpty() ? entry.getKey() : parent + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                extractFields((Map<String, Object>) value, fieldName, fields);
            } else {
                fields.add(fieldName);
            }
        }
    }
}
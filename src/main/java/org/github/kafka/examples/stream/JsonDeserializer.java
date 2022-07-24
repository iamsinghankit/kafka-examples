package org.github.kafka.examples.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

/**
 * @author iamsinghankit
 */
public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper jackson = new ObjectMapper();
    private Class<T> deserializedClass;

    public JsonDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (deserializedClass == null) {
            deserializedClass = (Class<T>) configs.get("serializedClass");
        }
    }


    @Override
    public void close() {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return jackson.readValue(data, deserializedClass);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

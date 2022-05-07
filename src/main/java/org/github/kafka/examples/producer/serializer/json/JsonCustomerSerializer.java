package org.github.kafka.examples.producer.serializer.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.github.kafka.examples.producer.serializer.simple.Customer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * @author iamsinghankit
 */
public class JsonCustomerSerializer implements Serializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to configure
    }

    @Override
    public void close() {
//        nothing to close
    }

    @Override
    public byte[] serialize(String topic, Customer data) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.writeValue(stream, data);
            return stream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw new SerializationException(e);
        }
    }
}

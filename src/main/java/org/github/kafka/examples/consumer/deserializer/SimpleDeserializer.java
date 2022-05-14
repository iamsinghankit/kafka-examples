package org.github.kafka.examples.consumer.deserializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.github.kafka.examples.producer.serializer.simple.Customer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @author iamsinghankit
 */
public class SimpleDeserializer implements Deserializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //nothing to configure
    }


    @Override
    public void close() {
        //nothing to close
    }

    @Override
    public Customer deserialize(String topic, byte[] data) {
        int id;
        int nameSize;
        String name;
        try {
            if (data == null)
                return null;
            if (data.length < 8) {
                throw new SerializationException("Size of data received by IntegerDeserializer is shorter than " +
                                                         "expected");
            }
            ByteBuffer buffer = ByteBuffer.wrap(data);
            id = buffer.getInt();
            nameSize = buffer.getInt();
            byte[] nameBytes = new byte[nameSize];
            buffer.get(nameBytes);
            name = new String(nameBytes, StandardCharsets.UTF_8);
            return new Customer(id, name);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing Customer to byte[]" + e);
        }
    }
}

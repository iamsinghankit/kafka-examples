package org.github.kafka.examples.producer.serializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @author iamsinghankit
 */
public class SimpleCustomerSerializer implements Serializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //nothing to configure
    }


    @Override
    public void close() {
        // nothing to close
    }

    @Override
    public byte[] serialize(String topic, Customer data) {
        try {
            byte[] serializedName = new byte[0];
            int stringSize = 0;
            if (data == null)
                return null;
            if (data.name() != null) {
                serializedName = data.name().getBytes(StandardCharsets.UTF_8);
                stringSize = serializedName.length;
            }
            var buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(data.id());
            buffer.putInt(stringSize);
            buffer.put(serializedName);
            return buffer.array();
        } catch (Exception ex) {
            throw new SerializationException("Error when serializing Customer to byte[]" + ex);

        }
    }
}

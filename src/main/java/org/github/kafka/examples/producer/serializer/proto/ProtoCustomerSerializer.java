package org.github.kafka.examples.producer.serializer.proto;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author iamsinghankit
 */
public class ProtoCustomerSerializer implements Serializer<Customer.CustomerProto> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to configure
    }

    @Override
    public void close() {
        // nothing to configure
    }

    @Override
    public byte[] serialize(String topic, Customer.CustomerProto data) {
        return data.toByteArray();
    }
}

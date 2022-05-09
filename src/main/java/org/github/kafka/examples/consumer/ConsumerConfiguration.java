package org.github.kafka.examples.consumer;

import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

/**
 * @author iamsinghankit
 */
public abstract class ConsumerConfiguration {

    protected Properties config() {
        var config = new Properties();
//        mandatory parameters
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        optional parameters
        config.put(GROUP_ID_CONFIG, "test-consumer-group");// consider this one mandatory.
        return config;
    }
}

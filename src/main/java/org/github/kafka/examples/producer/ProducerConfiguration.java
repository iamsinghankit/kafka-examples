package org.github.kafka.examples.producer;

import java.util.Properties;

/**
 * @author iamsinghankit
 */
public abstract class ProducerConfiguration {

    protected Properties config() {
        Properties properties = new Properties();
        // all 3 are mandatory
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }
}

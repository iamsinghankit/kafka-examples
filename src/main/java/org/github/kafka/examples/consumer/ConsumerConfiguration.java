package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.RoundRobinAssignor;
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
        config.put(FETCH_MIN_BYTES_CONFIG,300);
        config.put(FETCH_MAX_WAIT_MS_CONFIG,300);
        config.put(MAX_PARTITION_FETCH_BYTES_CONFIG,12121212);
        config.put(SESSION_TIMEOUT_MS_CONFIG,30000);
        config.put(AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(ENABLE_AUTO_COMMIT_CONFIG,false);
        config.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        config.put(CLIENT_ID_CONFIG,"test-client");
        config.put(MAX_POLL_RECORDS_CONFIG,200);
        config.put(RECEIVE_BUFFER_CONFIG,2323);
        config.put(SEND_BUFFER_CONFIG,232323);
        return config;
    }
}

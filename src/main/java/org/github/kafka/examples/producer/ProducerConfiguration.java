package org.github.kafka.examples.producer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * @author iamsinghankit
 */
public abstract class ProducerConfiguration {

    protected Properties config() {
        Properties prop = new Properties();
        // all 3 are mandatory
        prop.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // optional
        prop.put(ACKS_CONFIG, "1");
        prop.put(BUFFER_MEMORY_CONFIG,16384);
        prop.put(COMPRESSION_TYPE_CONFIG,"snappy");
        prop.put(RETRIES_CONFIG,2);
        prop.put(BATCH_SIZE_CONFIG,5);
        prop.put(LINGER_MS_CONFIG,1);
        prop.put(CLIENT_ID_CONFIG,"AsyncProducerClient");
        prop.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,20);
        prop.put(REQUEST_TIMEOUT_MS_CONFIG,10);
//        prop.put(MAX_BLOCK_MS_CONFIG,10);
        prop.put(MAX_REQUEST_SIZE_CONFIG,50000);
        prop.put(RECEIVE_BUFFER_CONFIG,-1);
        prop.put(SEND_BUFFER_CONFIG,-1);
        return prop;
    }
}

package org.github.kafka.examples;

import org.github.kafka.examples.consumer.Consumer;
import org.github.kafka.examples.consumer.SimpleConsumer;

/**
 * @author iamsinghankit
 */
public class KafkaConsumerApp {
    public static void main(String[] args) {
        consume(new SimpleConsumer());
    }

    private static void consume(Consumer consumer) {
        consumer.consume();
    }
}

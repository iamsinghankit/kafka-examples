package org.github.kafka.examples.consumer.deserializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.github.kafka.examples.consumer.Consumer;
import org.github.kafka.examples.consumer.ConsumerConfiguration;
import org.github.kafka.examples.producer.serializer.simple.Customer;

import java.time.Duration;
import java.util.List;

/**
 * @author iamsinghankit
 */
public class SimpleDeserializerConsumer extends ConsumerConfiguration implements Consumer {

    @Override
    public void consume() {
        var config = config();
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SimpleDeserializer.class.getName());
        System.out.println("Started Consuming...");
        try (var consumer = new KafkaConsumer<String, Customer>(config)) {
            consumer.subscribe(List.of("test"));

            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Customer> record : records) {
                    System.out.println(record.value().toString());
                }
            }

        }
    }
}

package org.github.kafka.examples.consumer.deserializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.github.kafka.examples.consumer.Consumer;
import org.github.kafka.examples.consumer.ConsumerConfiguration;
import org.github.kafka.examples.producer.serializer.avro.CustomerAvro;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * @author iamsinghankit
 */
public class AvroCustomerConsumer extends ConsumerConfiguration implements Consumer {

    @Override
    public void consume() {
        Properties config = config();
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        config.put("schema.registry.url", "http://localhost:8081");
        var consumer = new KafkaConsumer<String, CustomerAvro>(config);
        consumer.seekToEnd(consumer.assignment());
        consumer.subscribe(List.of("test"));
        System.out.println("Reading Topic: test");
        while (true) {
            ConsumerRecords<String, CustomerAvro> records = consumer.poll(Duration.ofMillis(100));
            for (var record : records) {
                System.out.println("Current customer name is: " + record.value().toString());
            }
            consumer.commitSync();

        }

    }
}

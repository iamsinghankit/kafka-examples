package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;

/**
 * @author iamsinghankit
 */
public class AsyncCommitConsumer extends ConsumerConfiguration implements Consumer {
    @Override
    public void consume() {
        var config = config();
        System.out.println("Started Consuming...");
        try (var consumer = new KafkaConsumer<String, String>(config)) {
            consumer.subscribe(List.of("test"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.toString());
                }
                try {
                    consumer.commitAsync();
                } catch (CommitFailedException ex) {
                    ex.printStackTrace();
                }
            }

        }
    }
}

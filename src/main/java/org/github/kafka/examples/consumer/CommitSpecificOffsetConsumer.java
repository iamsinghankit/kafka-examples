package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author iamsinghankit
 */
public class CommitSpecificOffsetConsumer extends ConsumerConfiguration implements Consumer {
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    int count=0;

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
                    currentOffsets.put(new TopicPartition(record.topic(),record.partition()),
                            new OffsetAndMetadata(record.offset()-10,"no metadata"));
//                    if(count % 1000==0)
                        consumer.commitAsync(currentOffsets,null);
                    count++;
                }
            }

        }
    }
}

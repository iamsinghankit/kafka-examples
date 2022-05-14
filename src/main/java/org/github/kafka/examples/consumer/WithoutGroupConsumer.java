package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author iamsinghankit
 */
public class WithoutGroupConsumer extends ConsumerConfiguration implements Consumer {
    @Override
    public void consume() {
        var config = config();
        System.out.println("Started Consuming...");
        var consumer = new KafkaConsumer<String, String>(config);
        List<PartitionInfo> partitionInfos = consumer.partitionsFor("test");
        List<TopicPartition> partitions = new ArrayList<>();

        if (!partitionInfos.isEmpty()) {
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
            }
            consumer.assign(partitions);
        }


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.toString());
            }
        }


    }
}

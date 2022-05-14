package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

/**
 * @author iamsinghankit
 */
public class SeekListeners extends ConsumerConfiguration implements Consumer {
    private final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config());

    @Override
    public void consume() {
        System.out.println("Starting Consuming..");
        consumer.subscribe(List.of("test"), new SaveOffsetsOnRebalance());
//        consumer.seekToBeginning(consumer.assignment());
        consumer.seekToEnd(consumer.assignment());
//        for (TopicPartition partition : consumer.assignment()) {
//            consumer.seek(partition, 14);
//        }
        consumer.poll(Duration.ofMillis(0));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.toString());
                //processRecord(record);
                //storeRecordInDB(record);
                //storeOffsetInDB(reord.topic(),record.partition(),record.offset());

            }
            //commitDbTranaction();
        }

    }

    private class SaveOffsetsOnRebalance implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            //do nothing
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            //commitDBTransaction();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                consumer.seek(partition, 14/*getOffsetFromDB(partition)*/);
            }
        }
    }
}

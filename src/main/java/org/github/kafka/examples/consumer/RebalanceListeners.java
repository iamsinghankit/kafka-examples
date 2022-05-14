package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author iamsinghankit
 */
public class RebalanceListeners extends ConsumerConfiguration implements Consumer {
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    int count=0;
    final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config());

    @Override
    public void consume() {
        var config = config();
        System.out.println("Started Consuming...");
        try (consumer) {
            consumer.subscribe(List.of("test"),new HandleBalance());

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.toString());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() - 10, "no metadata"));
//                    if(count % 1000==0)
                    consumer.commitAsync(currentOffsets, null);
                    count++;
                }
            }

        }catch (WakeupException ex){
            //ignore we're closing
        }
        finally {
            try{
                consumer.commitSync(currentOffsets);
            }finally {
                consumer.close();
                System.out.println("Closed consumer and we are done");
            }
        }
    }
    private class HandleBalance implements ConsumerRebalanceListener{
        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions) {
            //don't do anything
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            System.out.println("Lost partitions in rebalance Committing current Offsets: "+currentOffsets);
            consumer.commitSync(currentOffsets);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        }
    }
}

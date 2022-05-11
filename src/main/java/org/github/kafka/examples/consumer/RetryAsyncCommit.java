package org.github.kafka.examples.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author iamsinghankit
 */
public class RetryAsyncCommit extends ConsumerConfiguration implements Consumer {
    AtomicInteger atomicInteger = new AtomicInteger(0);

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
                    try {
                        consumer.commitAsync(new OffsetCommitCallback() {
                            private final int marker = atomicInteger.incrementAndGet();

                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                   Exception exception) {
                                if (exception != null) {
                                    if (marker == atomicInteger.get())
                                        consumer.commitAsync(this);
                                } else {
                                    //Can't retry anymore, newer commit already sent
                                }
                            }
                        });

                    } catch (CommitFailedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

}

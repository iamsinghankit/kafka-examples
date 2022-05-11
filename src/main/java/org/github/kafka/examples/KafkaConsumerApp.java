package org.github.kafka.examples;

import org.github.kafka.examples.consumer.CommitSpecificOffsetConsumer;
import org.github.kafka.examples.consumer.Consumer;
import org.github.kafka.examples.consumer.RetryAsyncCommit;

/**
 * @author iamsinghankit
 */
public class KafkaConsumerApp {
    public static void main(String[] args) {
//        consume(new SimpleConsumer());
//        consume(new AsyncCommitConsumer());
//        consume(new AsyncCommitCallbackConsumer());
//        consume(new SyncAndAsyncCommitConsumer());
//        consume(new CommitSpecificOffsetConsumer());
        consume(new RetryAsyncCommit());
    }

    private static void consume(Consumer consumer) {
        consumer.consume();
    }
}

package org.github.kafka.examples;

import org.github.kafka.examples.consumer.*;

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
//        consume(new RetryAsyncCommit());
//        consume(new RebalanceListeners());
//        consume(new SeekListeners());
        consume(new CleanExitConsumer());
    }

    private static void consume(Consumer consumer) {
        consumer.consume();
    }
}

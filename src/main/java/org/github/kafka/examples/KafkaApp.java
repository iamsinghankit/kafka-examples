package org.github.kafka.examples;

import org.github.kafka.examples.producer.AsyncProducer;
import org.github.kafka.examples.producer.FireAndForgetProducer;
import org.github.kafka.examples.producer.SyncProducer;
import org.github.kafka.examples.producer.serializer.SimpleProducer;

/**
 * @author Ankit Singh
 */
public class KafkaApp {
    public static void main(String[] args) {
        simpleProducer();
    }


    private static void fireAndForgetProducer() {
        var producer = new FireAndForgetProducer();
        producer.send();
    }

    private static void syncProducer() {
        var producer = new SyncProducer();
        producer.send();
    }

    private static void asyncProducer() {
        var producer = new AsyncProducer();
        producer.send();
    }

    private static void simpleProducer() {
        var producer = new SimpleProducer();
        producer.send();
    }

}

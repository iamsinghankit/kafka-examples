package org.github.kafka.examples;

import org.github.kafka.examples.producer.AsyncProducer;
import org.github.kafka.examples.producer.FireAndForgetProducer;
import org.github.kafka.examples.producer.Producer;
import org.github.kafka.examples.producer.SyncProducer;
import org.github.kafka.examples.producer.partition.SimplePartitionerProducer;
import org.github.kafka.examples.producer.serializer.SimpleSerializerProducer;

/**
 * @author Ankit Singh
 */
public class KafkaApp {
    public static void main(String[] args) {
        send(new SimplePartitionerProducer());
//        send(new FireAndForgetProducer());
//        send(new SyncProducer());
//        send(new AsyncProducer());
//        send(new SimpleSerializerProducer());
    }

    private static void send(Producer producer) {
        producer.send();
    }


}

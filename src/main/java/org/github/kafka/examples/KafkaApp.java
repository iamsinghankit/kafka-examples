package org.github.kafka.examples;

import org.github.kafka.examples.producer.Producer;
import org.github.kafka.examples.producer.serializer.avro.AvroSerializerProducer;
import org.github.kafka.examples.producer.serializer.simple.SimpleSerializerProducer;

/**
 * @author Ankit Singh
 */
public class KafkaApp {
    public static void main(String[] args) {
//        send(new SimplePartitionerProducer());
        send(new AvroSerializerProducer());
//        send(new SimpleSerializerProducer());
//        send(new FireAndForgetProducer());
//        send(new SyncProducer());
//        send(new AsyncProducer());

    }

    private static void send(Producer producer) {
        producer.send();
    }


}

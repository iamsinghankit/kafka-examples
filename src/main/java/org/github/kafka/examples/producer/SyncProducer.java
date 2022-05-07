package org.github.kafka.examples.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author iamsinghankit
 */
public class SyncProducer extends ProducerConfiguration implements Producer {

    @Override
    public void send() {
        var producer = new KafkaProducer<String, String>(config());
        var record = new ProducerRecord<String, String>("test", "Sync producer");
        try {
            producer.send(record).get();
            System.out.println("Message sent successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

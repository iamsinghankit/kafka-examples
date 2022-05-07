package org.github.kafka.examples.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author iamsinghankit
 */
public class AsyncProducer extends ProducerConfiguration {

    public void send() {
        var producer = new KafkaProducer<String, String>(config());
        var record = new ProducerRecord<String, String>("test", "Async producer");
        try {
            producer.send(record, (m, e) -> {
//                System.out.println("OffSet: "+m.offset());
                if (e != null)
                    e.printStackTrace();
            });
            //Waits for message sending before JVM terminates.
            Thread.sleep(500);
            System.out.println("Message sent successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

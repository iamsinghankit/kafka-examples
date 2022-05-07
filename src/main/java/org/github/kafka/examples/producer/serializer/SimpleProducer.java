package org.github.kafka.examples.producer.serializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.github.kafka.examples.producer.ProducerConfiguration;

import java.util.Properties;

/**
 * @author iamsinghankit
 */
public class SimpleProducer extends ProducerConfiguration {

    public void send(){
        Properties config = config();
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.github.kafka.examples.producer.serializer" +
                                                                        ".SimpleCustomerSerializer");
        var producer = new KafkaProducer<String, Customer>(config);
        var record = new ProducerRecord<String, Customer>("test", new Customer(1,"Ankit"));
        try {
            producer.send(record).get();
            System.out.println("Message sent successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
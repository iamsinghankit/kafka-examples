package org.github.kafka.examples.producer.partition;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.github.kafka.examples.producer.Producer;
import org.github.kafka.examples.producer.ProducerConfiguration;

import java.util.Properties;

/**
 * @author iamsinghankit
 */
public class SimplePartitionerProducer extends ProducerConfiguration implements Producer {

    @Override
    public void send() {
        Properties config = config();
        config.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.github.kafka.examples.producer.partition.CustomPartitioner");

        var producer = new KafkaProducer<String, String>(config);
        var record = new ProducerRecord<>("test", "Apple", "Macbook Pro");
        try {
            producer.send(record).get();
            System.out.println("Message sent successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package org.github.kafka.examples.producer.serializer.avro;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.github.kafka.examples.producer.Producer;
import org.github.kafka.examples.producer.ProducerConfiguration;

import java.util.Properties;

/**
 * @author iamsinghankit
 */
public class AvroSerializerProducer extends ProducerConfiguration implements Producer {

    @Override
    public void send() {
        Properties config = config();
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroCustomerSerializer.class.getName());

        var producer = new KafkaProducer<String, CustomerAvro>(config);
        var record = new ProducerRecord<String, CustomerAvro>("test", new CustomerAvro(1, "Ankit"));
        try {
            producer.send(record).get();
            System.out.println("Message sent successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

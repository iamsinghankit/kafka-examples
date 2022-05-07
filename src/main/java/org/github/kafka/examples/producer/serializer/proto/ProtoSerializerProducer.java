package org.github.kafka.examples.producer.serializer.proto;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.github.kafka.examples.producer.Producer;
import org.github.kafka.examples.producer.ProducerConfiguration;

import java.util.Properties;

/**
 * @author iamsinghankit
 */
public class ProtoSerializerProducer extends ProducerConfiguration implements Producer {

    @Override
    public void send() {
        Properties config = config();
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtoCustomerSerializer.class.getName());

        Customer.CustomerProto customer = Customer.CustomerProto.newBuilder()
                                                                .setId(1)
                                                                .setName("Ankit")
                                                                .build();

        var producer = new KafkaProducer<String, Customer.CustomerProto>(config);
        var record = new ProducerRecord<String, Customer.CustomerProto>("test", customer);
        try {
            producer.send(record).get();
            System.out.println("Message sent successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

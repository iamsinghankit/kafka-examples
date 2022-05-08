package org.github.kafka.examples.producer.serializer.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
        config.put("schema.registry.url","http://localhost:8081");
//        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroCustomerSerializer.class.getName());

        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        var producer = new KafkaProducer<String, CustomerAvro>(config);
        CustomerAvro customer = CustomerAvro.newBuilder()
                                            .setId(1)
                                            .setName("Ankit")
                                            .build();
        var record = new ProducerRecord<String, CustomerAvro>("test", customer);
        try {
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println(recordMetadata.offset());
            System.out.println("Message sent successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

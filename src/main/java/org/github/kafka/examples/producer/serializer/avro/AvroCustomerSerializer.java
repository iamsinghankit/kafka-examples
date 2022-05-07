package org.github.kafka.examples.producer.serializer.avro;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * @author iamsinghankit
 */
public class AvroCustomerSerializer implements Serializer<CustomerAvro> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
//      nothing to configure
    }


    @Override
    public void close() {
//       nothing to close
    }

    @Override
    public byte[] serialize(String topic, CustomerAvro customer) {
        DatumWriter<CustomerAvro> writer = new SpecificDatumWriter<>(CustomerAvro.class);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
           Encoder jsonEncoder = EncoderFactory.get().binaryEncoder(stream, null);
            writer.write(customer, jsonEncoder);
            jsonEncoder.flush();
            return stream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            throw  new SerializationException(e);
        }
    }
}

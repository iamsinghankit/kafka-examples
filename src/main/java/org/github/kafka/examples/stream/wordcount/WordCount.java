package org.github.kafka.examples.stream.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 *  1. Create a wordcount-input topic:
 *     bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wordcount-input --partitions 1 --replication-factor 1
 *
 *  2.Produce some text to the topic. Don't forget to repeat words (so we can count higher than 1) and to use the word "the", so we can filter it.
 *    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic wordcount-input
 *
 *  3.Take a look at the results:
 *     bin/kafka-console-consumer.sh --topic wordcount-output --from-beginning --bootstrap-server localhost:9092 --property print.key=true
 *
 *  If you want to reset state and re-run the application (maybe with some changes?) on existing input topic, you can:
 *
 *  1. Reset internal topics (used for shuffle and state-stores):
 *      bin/kafka-streams-application-reset.sh --application-id wordcount --bootstrap-servers localhost:9092 --input-topics wordcount-input
 *
 *  2. (optional) Delete the output topic:
 *     bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic wordcount-output
 *
 * @author iamsinghankit
 */
public class WordCount implements Runnable {
    @Override
    public void run() {
        //defining configuration for streams, optionally producer and consumer
        Properties config = config();

        //creates a topology
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("wordcount-input");

        Pattern pattern = Pattern.compile("\\W+");

        KStream<String, String> counts = source.flatMapValues(value -> List.of(pattern.split(value.toLowerCase())))
                                                 .map((key, value) -> new KeyValue<>(value, value))
                                                 .filter((key, value) -> (!value.equals("the")))
                                                 .groupByKey()
                                                 .count(Named.as("CountStore"))
                                                 .mapValues(value -> Long.toString(value)).toStream();
        counts.to("wordcount-output");
        Topology topology = builder.build();

        //executes the topology
        KafkaStreams kafkaStreams = new KafkaStreams(topology, config);
        kafkaStreams.start();
        sleepQuietly(Duration.ofSeconds(5));
        kafkaStreams.close();

    }

    private void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
        }
    }

    private Properties config() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return config;
    }




}

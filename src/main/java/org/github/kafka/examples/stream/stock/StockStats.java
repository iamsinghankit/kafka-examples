package org.github.kafka.examples.stream.stock;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.github.kafka.examples.stream.Constants;
import org.github.kafka.examples.stream.JsonDeserializer;
import org.github.kafka.examples.stream.JsonSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author iamsinghankit
 */
public class StockStats implements Runnable {
    @Override
    public void run() {

        Properties props = config();
        // Time interval, in millisecond, for our aggregation window
        long windowSize = 5000;

        // creating an AdminClient and checking the number of brokers in the cluster, so I'll know how many replicas we want...

        AdminClient ac = AdminClient.create(props);
        DescribeClusterResult dcr = ac.describeCluster();
        int clusterSize = 0;
        try {
            clusterSize = dcr.nodes().get().size();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        props.put("replication.factor", Math.min(clusterSize, 3));

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trade> source = builder.stream(Constants.STOCK_TOPIC);

        KStream<Windowed<String>, TradeStats> stats = source.groupByKey()
                                                              .windowedBy(TimeWindows.of(Duration.ofMillis(windowSize)).advanceBy(Duration.ofSeconds(1)))
                                                              .aggregate(TradeStats::new, (k, v, tradestats) -> tradestats.add(v), Materialized.<String, TradeStats, WindowStore<Bytes, byte[]>>as("trade-aggregates").withValueSerde(new TradeStatsSerde()))
                                                              .toStream()
                                                              .mapValues(TradeStats::computeAvgPrice);

        stats.to("stockstats-output", Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize)));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        System.out.println(topology.describe());

        streams.cleanUp();

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private Properties config(){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stockstat");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,Constants.BROKER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TradeSerde.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static public final class TradeSerde extends Serdes.WrapperSerde<Trade> {
        public TradeSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Trade.class));
        }
    }

    static public final class TradeStatsSerde extends Serdes.WrapperSerde<TradeStats> {
        public TradeStatsSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(TradeStats.class));
        }
    }
}

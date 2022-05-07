package org.github.kafka.examples.producer.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * @author iamsinghankit
 */
public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> testPartitions = cluster.partitionsForTopic("test");

        if (keyBytes == null) {
            throw new InvalidRecordException("Invalid key");
        }
        //Apple is customer
        if (key instanceof String s && s.equals("Apple")) {
            return testPartitions.get(0).partition(); //Apple with always go to first partition
        }
        //Other records will get hashed to the rest of the partitions
        return (Math.abs(Utils.murmur2(keyBytes)) % (testPartitions.size() < 2 ? 1 : testPartitions.size() - 1));

    }

    @Override
    public void close() {
        //nothing to close
    }

    @Override
    public void configure(Map<String, ?> configs) {
        //nothing to configure
    }
}

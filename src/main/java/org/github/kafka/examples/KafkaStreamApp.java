package org.github.kafka.examples;

import org.github.kafka.examples.stream.click.ClickStreamEnrichment;

/**
 * @author iamsinghankit
 */
public class KafkaStreamApp {
    public static void main(String[] args) {
//        run(new WordCount());
//        run(new StockStats());
        run(new ClickStreamEnrichment());
    }

    private static void run(Runnable runnable) {
        runnable.run();
    }
}

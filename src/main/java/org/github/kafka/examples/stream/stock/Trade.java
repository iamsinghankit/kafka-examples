package org.github.kafka.examples.stream.stock;

public record Trade(String type, String ticker, double price, int size) {
}
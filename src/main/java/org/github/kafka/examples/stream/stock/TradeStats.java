package org.github.kafka.examples.stream.stock;

public class TradeStats {

    private String type;
    private String ticker;
    private int countTrades; // tracking count and sum so we can later calculate avg price
    private  double sumPrice;
    private  double minPrice;
    private double avgPrice;

    public TradeStats add(Trade trade) {
        if (trade.type() == null || trade.ticker() == null)
            throw new IllegalArgumentException("Invalid trade to aggregate: " + trade);

        if (this.type == null)
            this.type = trade.type();

        if (this.ticker == null)
            this.ticker = trade.ticker();

        if (!this.type.equals(trade.type()) || !this.ticker.equals(trade.ticker()))
            throw new IllegalArgumentException("Aggregating stats for trade type " + this.type + " and ticker " + this.ticker + " but received trade of type " + trade.type()
                                                       + " and ticker " + trade.ticker());

        if (countTrades == 0)
            this.minPrice = trade.price();

        this.countTrades = this.countTrades + 1;
        this.sumPrice = this.sumPrice + trade.price();
        this.minPrice = this.minPrice < trade.price() ? this.minPrice : trade.price();

        return this;
    }

    public TradeStats computeAvgPrice() {
        this.avgPrice = this.sumPrice / this.countTrades;
        return this;
    }


    public String getType() {
        return type;
    }

    public TradeStats setType(String type) {
        this.type = type;
        return this;
    }

    public String getTicker() {
        return ticker;
    }

    public TradeStats setTicker(String ticker) {
        this.ticker = ticker;
        return this;
    }

    public int getCountTrades() {
        return countTrades;
    }

    public TradeStats setCountTrades(int countTrades) {
        this.countTrades = countTrades;
        return this;
    }

    public double getSumPrice() {
        return sumPrice;
    }

    public TradeStats setSumPrice(double sumPrice) {
        this.sumPrice = sumPrice;
        return this;
    }

    public double getMinPrice() {
        return minPrice;
    }

    public TradeStats setMinPrice(double minPrice) {
        this.minPrice = minPrice;
        return this;
    }

    public double getAvgPrice() {
        return avgPrice;
    }

    public TradeStats setAvgPrice(double avgPrice) {
        this.avgPrice = avgPrice;
        return this;
    }
}
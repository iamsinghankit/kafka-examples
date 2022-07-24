package org.github.kafka.examples.stream;

public class Constants {
    public static final String STOCK_TOPIC = "stocks";
    public static final String[] TICKERS = {"MMM", "ABT", "ABBV", "ACN", "ATVI", "AYI", "ADBE", "AAP", "AES", "AET"};
    public static final int MAX_PRICE_CHANGE = 5;
    public static final int START_PRICE = 5000;
    public static final int DELAY = 100; // sleep in ms between sending "asks"
    public static final String BROKER = "localhost:9092";

    public static final String USER_PROFILE_TOPIC = "clicks.user.profile";
    public static final String PAGE_VIEW_TOPIC = "clicks.pages.views";
    public static final String SEARCH_TOPIC = "clicks.search";
    public static final String USER_ACTIVITY_TOPIC = "clicks.user.activity";

}
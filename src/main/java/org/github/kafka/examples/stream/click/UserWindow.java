package org.github.kafka.examples.stream.click;

public class UserWindow {
    private int userId;
    private long timestamp;

    public UserWindow() {
    }

    public UserWindow(int userId, long timestamp) {
        this.userId = userId;
        this.timestamp = timestamp;
    }

    public int getUserId() {
        return userId;
    }

    public UserWindow setUserId(int userId) {
        this.userId = userId;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public UserWindow setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }
}
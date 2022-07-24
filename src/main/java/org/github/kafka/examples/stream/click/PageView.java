package org.github.kafka.examples.stream.click;

/**
 * @author iamsinghankit
 */
public class PageView{
    private int userId;
    private String page;

    public PageView() {
    }

    public PageView(int userId, String page) {
        this.userId = userId;
        this.page = page;
    }

    public int getUserId() {
        return userId;
    }

    public PageView setUserId(int userId) {
        this.userId = userId;
        return this;
    }

    public String getPage() {
        return page;
    }

    public PageView setPage(String page) {
        this.page = page;
        return this;
    }
}

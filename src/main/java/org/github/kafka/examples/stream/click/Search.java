package org.github.kafka.examples.stream.click;

/**
 * @author iamsinghankit
 */
public class Search {
    private int userId;
    private String searchTerms;

    public Search() {
    }

    public Search(int userId, String searchTerms) {
        this.userId = userId;
        this.searchTerms = searchTerms;
    }

    public int getUserId() {
        return userId;
    }

    public Search setUserId(int userId) {
        this.userId = userId;
        return this;
    }

    public String getSearchTerms() {
        return searchTerms;
    }

    public Search setSearchTerms(String searchTerms) {
        this.searchTerms = searchTerms;
        return this;
    }
}

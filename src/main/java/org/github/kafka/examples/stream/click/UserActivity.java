package org.github.kafka.examples.stream.click;

public class UserActivity {
   private int userId;
    private String userName;
    private String zipcode;
    private String[] interests;
    private String searchTerm;
    private String page;

    public UserActivity() {
    }

    public UserActivity(int userId, String userName, String zipcode, String[] interests, String searchTerm, String page) {
        this.userId = userId;
        this.userName = userName;
        this.zipcode = zipcode;
        this.interests = interests;
        this.searchTerm = searchTerm;
        this.page = page;
    }

    public UserActivity updateSearch(String searchTerm) {
        this.searchTerm = searchTerm;
        return this;
    }

    public int getUserId() {
        return userId;
    }

    public UserActivity setUserId(int userId) {
        this.userId = userId;
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public UserActivity setUserName(String userName) {
        this.userName = userName;
        return this;
    }

    public String getZipcode() {
        return zipcode;
    }

    public UserActivity setZipcode(String zipcode) {
        this.zipcode = zipcode;
        return this;
    }

    public String[] getInterests() {
        return interests;
    }

    public UserActivity setInterests(String[] interests) {
        this.interests = interests;
        return this;
    }

    public String getSearchTerm() {
        return searchTerm;
    }

    public UserActivity setSearchTerm(String searchTerm) {
        this.searchTerm = searchTerm;
        return this;
    }

    public String getPage() {
        return page;
    }

    public UserActivity setPage(String page) {
        this.page = page;
        return this;
    }
}
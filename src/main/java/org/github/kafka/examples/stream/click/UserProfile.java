package org.github.kafka.examples.stream.click;

public class UserProfile {
    private  int userID;
    private String userName;
    private String zipcode;
    private String[] interests;

    public UserProfile() {
    }

    public UserProfile(int userID, String userName, String zipcode, String[] interests) {
        this.userID = userID;
        this.userName = userName;
        this.zipcode = zipcode;
        this.interests = interests;
    }

    public int getUserID() {
        return userID;
    }

    public UserProfile update(String zipcode, String[] interests) {
        this.zipcode = zipcode;
        this.interests = interests;
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public String getZipcode() {
        return zipcode;
    }

    public String[] getInterests() {
        return interests;
    }
}

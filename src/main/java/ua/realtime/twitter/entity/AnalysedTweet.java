package ua.realtime.twitter.entity;

import java.io.Serializable;

/**
 * Created by alukard on 5/1/15.
 */
public class AnalysedTweet implements Serializable {

    private static final long serialVersionUID = -2776527864326689622L;

    private String text;
    private String userName;
    private Double sentiment;

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Double getSentiment() {
        return sentiment;
    }

    public void setSentiment(Double sentiment) {
        this.sentiment = sentiment;
    }
}

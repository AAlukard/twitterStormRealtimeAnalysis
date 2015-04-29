package ua.realtime.twitter.entity;

import java.io.Serializable;

/**
 * Created by alukard on 3/14/15.
 */
public class ParsedTweet implements Serializable {
    private static final long serialVersionUID = -8966122129224494844L;

    private String text;
    private String userName;

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

    //    private List<String> tags;

    //coordinates
    //created
    //entities
    //-hastags
    //id_str
    //retweet_count
    //user info
    //withheld_in_countries ?
}

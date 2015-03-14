package ua.realtime.twitter.util;

import java.io.Serializable;

/**
 * Created by alukard on 3/14/15.
 */
public class OauthCredentials implements Serializable{

    private static final long serialVersionUID = -1531911596830762775L;

    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessSecrete;

    public String getConsumerKey() {
        return consumerKey;
    }

    public void setConsumerKey(String consumerKey) {
        this.consumerKey = consumerKey;
    }

    public String getConsumerSecret() {
        return consumerSecret;
    }

    public void setConsumerSecret(String consumerSecret) {
        this.consumerSecret = consumerSecret;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public String getAccessSecret() {
        return accessSecrete;
    }

    public void setAccessSecret(String accessSecrete) {
        this.accessSecrete = accessSecrete;
    }
}

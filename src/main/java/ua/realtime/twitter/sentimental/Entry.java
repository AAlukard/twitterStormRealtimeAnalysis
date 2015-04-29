package ua.realtime.twitter.sentimental;

/**
 * Created by alukard on 4/29/15.
 */
public class Entry {

    private String word;
    private String happinessRank;
    private String happinessAverage;
    private String happinessStandardDeviation;
    private String twitterRank;
    private String googleRank;
    private String nytRank;
    private String lyricsRank;

    public Entry(String word, String happinessRank, String happinessAverage, String happinessStandardDeviation, String twitterRank, String googleRank, String nytRank, String lyricsRank) {
        this.word = word;
        this.happinessRank = happinessRank;
        this.happinessAverage = happinessAverage;
        this.happinessStandardDeviation = happinessStandardDeviation;
        this.twitterRank = twitterRank;
        this.googleRank = googleRank;
        this.nytRank = nytRank;
        this.lyricsRank = lyricsRank;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getHappinessRank() {
        return happinessRank;
    }

    public void setHappinessRank(String happinessRank) {
        this.happinessRank = happinessRank;
    }

    public String getHappinessAverage() {
        return happinessAverage;
    }

    public void setHappinessAverage(String happinessAverage) {
        this.happinessAverage = happinessAverage;
    }

    public String getHappinessStandardDeviation() {
        return happinessStandardDeviation;
    }

    public void setHappinessStandardDeviation(String happinessStandardDeviation) {
        this.happinessStandardDeviation = happinessStandardDeviation;
    }

    public String getTwitterRank() {
        return twitterRank;
    }

    public void setTwitterRank(String twitterRank) {
        this.twitterRank = twitterRank;
    }

    public String getGoogleRank() {
        return googleRank;
    }

    public void setGoogleRank(String googleRank) {
        this.googleRank = googleRank;
    }

    public String getNytRank() {
        return nytRank;
    }

    public void setNytRank(String nytRank) {
        this.nytRank = nytRank;
    }

    public String getLyricsRank() {
        return lyricsRank;
    }

    public void setLyricsRank(String lyricsRank) {
        this.lyricsRank = lyricsRank;
    }
}

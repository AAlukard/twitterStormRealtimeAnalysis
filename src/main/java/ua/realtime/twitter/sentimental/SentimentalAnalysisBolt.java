package ua.realtime.twitter.sentimental;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ua.realtime.twitter.entity.AnalysedTweet;
import ua.realtime.twitter.entity.ParsedTweet;

import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by alukard on 4/30/15.
 */
public class SentimentalAnalysisBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(SentimentalAnalysisBolt.class);

    private Map<String, Entry> dictionary;

    private OutputCollector collector;

    public SentimentalAnalysisBolt(Map<String, Entry> dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        ParsedTweet parsedTweet = (ParsedTweet) input.getValueByField("parsed-tweet");
        StringTokenizer tokenizer = new StringTokenizer(parsedTweet.getText());

        int count = 0;
        Double sentiment = 0d;
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken().toLowerCase();

            if (dictionary.containsKey(token)) {
                sentiment += dictionary.get(token).getHappinessAverage();
                count++;
            }
        }

        if (count != 0) {
            sentiment = sentiment/count;
        }

        AnalysedTweet tweet = new AnalysedTweet();
        tweet.setText(parsedTweet.getText());
        tweet.setUserName(parsedTweet.getUserName());
        tweet.setSentiment(sentiment);

        String term = input.getStringByField("term");

        collector.emit(new Values(term, tweet));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("term", "analysed-tweet"));
    }
}

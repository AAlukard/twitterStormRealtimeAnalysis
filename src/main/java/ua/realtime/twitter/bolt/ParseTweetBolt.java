package ua.realtime.twitter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;
import ua.realtime.twitter.entity.ParsedTweet;

import java.util.Map;

/**
 * Created by alukard on 3/14/15.
 */
public class ParseTweetBolt extends BaseRichBolt{

    private static final Logger LOG = LoggerFactory.getLogger(ParseTweetBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Status tweet = (Status) input.getValue(0);

        ParsedTweet parsedTweet = new ParsedTweet();
        parsedTweet.setText(tweet.getText());
        parsedTweet.setUserName(tweet.getUser().getName());
        //TODO: implement

        collector.emit(new Values(parsedTweet));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("parsed-tweet"));
    }
}

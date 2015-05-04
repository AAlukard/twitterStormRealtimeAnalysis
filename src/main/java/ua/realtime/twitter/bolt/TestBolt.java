package ua.realtime.twitter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ua.realtime.twitter.entity.AnalysedTweet;

import java.util.Map;

/**
 * Created by alukard on 3/14/15.
 */
public class TestBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(TestBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    @Override
    public void execute(Tuple tuple) {
//        AnalysedTweet tweet = (AnalysedTweet) tuple.getValue(0);
//        LOG.info("###-" + tweet.getUserName() + " : " + tweet.getText() + ":" + tweet.getSentiment() + "-###");


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

package ua.realtime.twitter.mentions;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ua.realtime.twitter.entity.ParsedTweet;

import java.util.Map;

/**
 * Created by alukard on 5/2/15.
 */
public class CountBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(CountBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        ParsedTweet parsedTweet = (ParsedTweet) input.getValueByField("parsed-tweet");
        String term = input.getStringByField("term");

        LOG.info(String.format("For term __%s__ got text: %s", term, parsedTweet.getText()));

        collector.emit(new Values(term, new Integer(1)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("term", "count"));
    }
}

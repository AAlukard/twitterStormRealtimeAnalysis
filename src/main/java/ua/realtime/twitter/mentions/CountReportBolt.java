package ua.realtime.twitter.mentions;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by alukard on 5/2/15.
 */
public class CountReportBolt extends BaseRichBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    @Override
    public void execute(Tuple input) {


        // push to messages queue
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

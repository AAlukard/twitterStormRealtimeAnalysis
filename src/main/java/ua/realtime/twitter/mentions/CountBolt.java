package ua.realtime.twitter.mentions;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.util.TupleHelpers;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by alukard on 5/4/15.
 */
public class CountBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(CountBolt.class);

    private int emitFrequency;
    private OutputCollector collector;

    Map<String, Long> countMap;

    public CountBolt(int emitFrequency) {
        this.emitFrequency = emitFrequency;
        countMap = new HashMap<>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {

        if (TupleHelpers.isTickTuple(input)) {
            LOG.info("Received tick tuple, triggering emit of current counter map");

            for (Map.Entry<String, Long> entry : countMap.entrySet()) {
                collector.emit(new Values(entry.getKey(), entry.getValue()));
            }
            countMap = new HashMap<>();
            return;
        }

        String term = input.getStringByField("term");

        Long counter = countMap.get(term);
        if (counter == null) {
            counter = new Long(0);
            countMap.put(term, counter);
        }
        counter = counter + 1;
//        LOG.info("for term: " + term + " saving count: " + counter);

        countMap.put(term, counter);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }
}

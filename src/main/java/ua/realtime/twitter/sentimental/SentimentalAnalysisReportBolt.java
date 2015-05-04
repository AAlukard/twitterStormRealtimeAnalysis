package ua.realtime.twitter.sentimental;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.util.TupleHelpers;
import ua.realtime.twitter.entity.AnalysedTweet;

import java.util.*;

/**
 * Created by alukard on 5/3/15.
 */
public class SentimentalAnalysisReportBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(SentimentalAnalysisReportBolt.class);

    private MongoClient mongoClient;
    private MongoCollection<Document> coll;

    private Map<String, List<AnalysedTweet>> termMap;

    private int emitFrequency;

    public SentimentalAnalysisReportBolt(int emitFrequency) {
        this.emitFrequency = emitFrequency;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        termMap = new HashMap<>();

        // add host, port and other stuff
        mongoClient = new MongoClient();
        coll = mongoClient.getDatabase("twitterRealTimeAnalysis").getCollection("sentimentalMessages");
    }

    @Override
    public void execute(Tuple tuple) {

        if (TupleHelpers.isTickTuple(tuple)) {
            LOG.info("Received tick tuple, triggering emit of current sentimental map");
            publishReport();
            return;
        }

        AnalysedTweet tweet = (AnalysedTweet) tuple.getValueByField("analysed-tweet");
        String term = tuple.getStringByField("term");

        List<AnalysedTweet> termList = termMap.get(term);
        if (termMap.get(term) == null) {
            termList = new LinkedList<>();
            termMap.put(term, termList);
        }

        termList.add(tweet);
    }

    private void publishReport() {
        Date currentDate = new Date();

        for (String term : termMap.keySet()) {
            List<AnalysedTweet> termList = termMap.get(term);

            double sentiment = 0;
            int count = 0;

            for (AnalysedTweet tweet : termList) {

                if (tweet.getSentiment() == 0) {
                    continue;
                }

                sentiment += tweet.getSentiment();
                count++;
            }

            if (count != 0) {
                sentiment = sentiment/count;
            }

            // todo: insert all?
            Document newDoc = new Document();
            newDoc.append("term", term)
                    .append("sentiment", sentiment)
                    .append("time", currentDate.getTime());
            coll.insertOne(newDoc);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

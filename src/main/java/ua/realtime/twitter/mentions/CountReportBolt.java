package ua.realtime.twitter.mentions;

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

import java.util.Date;
import java.util.Map;

/**
 * Created by alukard on 5/2/15.
 */
public class CountReportBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(CountReportBolt.class);

    private MongoClient mongoClient;
    private MongoCollection<Document> mentionsMessagesCollection;
    private MongoCollection<Document> mentionsStorageCollection;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        // add host, port and other stuff
        mongoClient = new MongoClient();
        mentionsMessagesCollection = mongoClient.getDatabase("twitterRealTimeAnalysis").getCollection("mentionMessages");
        mentionsStorageCollection = mongoClient.getDatabase("twitterRealTimeAnalysis").getCollection("mentions");
    }

    @Override
    public void execute(Tuple tuple) {
        String term = tuple.getStringByField("obj");
        long count = tuple.getLongByField("count");

        LOG.debug("===============================");

        LOG.debug(String.format("Word: __%s__, count: [%d]", term, count));

        LOG.debug("===============================");

        Document newDoc = new Document();
        newDoc.append("term", term)
                .append("count", count)
                .append("time", new Date());
        mentionsMessagesCollection.insertOne(newDoc);
        mentionsStorageCollection.insertOne(newDoc);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

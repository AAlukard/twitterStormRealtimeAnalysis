package ua.realtime.twitter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import ua.realtime.twitter.util.BasicStatusListener;
import ua.realtime.twitter.util.OauthCredentials;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by alukard on 3/14/15.
 */
public class TweetSpout extends BaseRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(TweetSpout.class);

    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;
    private SpoutOutputCollector spoutOutputCollector;

    private OauthCredentials oauthCredentials;
    public TweetSpout(OauthCredentials oauthCredentials) {
        this.oauthCredentials = oauthCredentials;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        this.spoutOutputCollector = spoutOutputCollector;

        queue = new LinkedBlockingQueue<>(1000);

        ConfigurationBuilder config =
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(oauthCredentials.getConsumerKey())
                        .setOAuthConsumerSecret(oauthCredentials.getConsumerSecret())
                        .setOAuthAccessToken(oauthCredentials.getAccessToken())
                        .setOAuthAccessTokenSecret(oauthCredentials.getAccessSecret());

        TwitterStreamFactory fact = new TwitterStreamFactory(config.build());

        twitterStream = fact.getInstance();

        twitterStream.addListener(new TweetListener());

        FilterQuery filterQuery = new FilterQuery();
        filterQuery.track(new String[]{"Nike", "Adidas"});
        twitterStream.filter(filterQuery);
    }

    @Override
    public void nextTuple() {
        Status tweet = queue.poll();

        if (tweet == null) {
            Utils.sleep(50);
            return;
        }

        spoutOutputCollector.emit(new Values(tweet));
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    private class TweetListener extends BasicStatusListener {

        @Override
        public void onStatus(Status status) {
//            LOG.info("Size of queue: {}", queue.size());
            queue.offer(status);
        }
    }
}

package ua.realtime.twitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ua.realtime.twitter.bolt.ParseTweetBolt;
import ua.realtime.twitter.bolt.TestBolt;
import ua.realtime.twitter.spout.TweetSpout;
import ua.realtime.twitter.util.OauthCredentials;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by alukard on 3/14/15.
 */
public class Topology {

    private static final Logger LOG = LoggerFactory.getLogger(Topology.class);

    public static void main(String[] args) throws IOException {

        String mode = "local";
        if (args != null && args.length > 0) {
            LOG.info("Creating topology with next parameters from cl: " + Arrays.toString(args));
            mode = args[0];
        }

        OauthCredentials oauthCredentials = readTwitterCredentials(mode);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("tweet-spout", new TweetSpout(oauthCredentials));
        builder.setBolt("parsed-tweet", new ParseTweetBolt()).shuffleGrouping("tweet-spout");
        builder.setBolt("test-bolt", new TestBolt()).globalGrouping("parsed-tweet");

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("tweet", conf, builder.createTopology());
        Utils.sleep(300000);
        cluster.killTopology("tweet");
        cluster.shutdown();
    }

    private static OauthCredentials readTwitterCredentials(final String mode) throws IOException {
        Properties prop = new Properties();
        final String propsName = mode + ".properties";
        LOG.info("Reading properties file: " + propsName);
        InputStream inputStream = Topology.class.getClassLoader().getResourceAsStream(propsName);

        if (inputStream != null) {
            prop.load(inputStream);
        } else {
            throw new FileNotFoundException("property file '" + propsName + "' not found in the classpath");
        }

        String consumerKey = prop.getProperty(Constants.Twitter.PROPERTY_CONSUMER_KEY);
        String consumerSecret = prop.getProperty(Constants.Twitter.PROPERTY_CONSUMER_SECRET);
        String accessToken = prop.getProperty(Constants.Twitter.PROPERTY_ACCESS_TOKEN);
        String accessSecret = prop.getProperty(Constants.Twitter.PROPERTY_ACCESS_SECRET);

        LOG.info(Constants.Twitter.PROPERTY_CONSUMER_KEY + ":" + consumerKey);
        LOG.info(Constants.Twitter.PROPERTY_CONSUMER_SECRET + ":" + consumerSecret);
        LOG.info(Constants.Twitter.PROPERTY_ACCESS_TOKEN + ":" + accessToken);
        LOG.info(Constants.Twitter.PROPERTY_ACCESS_SECRET + ":" + accessSecret);

        OauthCredentials oauthCredentials = new OauthCredentials();
        oauthCredentials.setAccessSecret(accessSecret);
        oauthCredentials.setAccessToken(accessToken);
        oauthCredentials.setConsumerKey(consumerKey);
        oauthCredentials.setConsumerSecret(consumerSecret);

        return oauthCredentials;
    }
}

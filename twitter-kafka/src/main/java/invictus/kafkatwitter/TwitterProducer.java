package invictus.kafkatwitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {


    private static InputStream inputStream;
    Logger logger= LoggerFactory.getLogger(TwitterProducer.class.getName());


    // Keys required for twitter stream
    static String consumerKey;
    static String consumerSecret;
    static String token;
    static String secret;

    public static void main(String[] args) throws FileNotFoundException {

        readConfigValues();
        new TwitterProducer().run();

    }

    private static void readConfigValues() throws FileNotFoundException {

        Properties prop = new Properties();
        String propFileName = "config.properties";

        inputStream = TwitterProducer.class.getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            try {
                prop.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }
        consumerKey=prop.getProperty("consumerKey");
        consumerSecret=prop.getProperty("consumerSecret");
        secret=prop.getProperty("secret");
        token=prop.getProperty("token");

    }

    public void run(){

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        // Create twitter client
        Client twitterClient=createTwitterClient(msgQueue);

        twitterClient.connect();

        KafkaProducer<String,String>producer=createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Stopping Application...");
            logger.info("Shutting Down client from twitter");
            twitterClient.stop();
            logger.info("Closed application");
            producer.close();
        }));
        while (!twitterClient.isDone()) {
            String msg = null;

            // Get Msg. from twitter
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }

            // Send Msg. to kafka
            if(msg!=null){
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("Something bad happened");

                        }
                    }
                });
            }

        }
        logger.info("End of application");
        // Create Kafka Producer


        // Loop to send tweets to kafka
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue ){

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("India","Pak","world cup");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();

    }

    public KafkaProducer<String,String>createKafkaProducer(){
        String bootstrapServer="127.0.0.1:9092";

        // Set Config properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        // Kafka 2.0 >=1.1 so we can keep this as 5. Use 1 otherwise

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
        // Create Producer

        return new KafkaProducer<String, String>(properties);
    }
}

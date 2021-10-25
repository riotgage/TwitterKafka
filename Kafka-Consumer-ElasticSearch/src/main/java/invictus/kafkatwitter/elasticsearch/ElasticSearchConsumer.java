package invictus.kafkatwitter.elasticsearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static InputStream inputStream;

    static String hostname="";
    static String username="";
    static String password="";
    public static RestHighLevelClient createClient(){

//        https://mbar3ru5w7:if4xfio8q7@twitterkafka-5346966143.ap-southeast-2.bonsaisearch.net:443


        final CredentialsProvider credentialsProvider=new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));

        RestClientBuilder builder= RestClient.builder(new HttpHost(
                hostname,443,"https"
        )).setHttpClientConfigCallback(
                new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                }
        );
        RestHighLevelClient client=new RestHighLevelClient(builder);
        return  client;
    }
    public static void main(String[] args) throws IOException {

        Logger logger= LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        readConfigValues();

        RestHighLevelClient client=createClient();

        KafkaConsumer<String,String>consumer=createConsumer("twitter_tweets");

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            Integer recordCount=records.count();
            logger.info("Received "+recordCount+" records");

            BulkRequest bulkRequest=new BulkRequest();
            for (ConsumerRecord<String, String> record : records) {
                String json=record.value();

                // Kafka Generic ID
//                String id=record.topic()+"_"+record.partition()+"_"+record.offset();
                try{
                    String id=extracIDFromTweet(record.value());
                    IndexRequest indexRequest=new IndexRequest("twitter","tweets",id).source(json, XContentType.JSON);

                    bulkRequest.add(indexRequest);
                }catch (NullPointerException e){
                    logger.warn("Skipping bad data: "+record.value() );
                }


            }
            if(recordCount>0){
                BulkResponse bulkItemResponses=client.bulk(bulkRequest,RequestOptions.DEFAULT);
                logger.info("Commiting Offsets....");
                consumer.commitSync();
                logger.info("offsets have been commited");
                try{
                    Thread.sleep(1000);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }

        }
//        client.close();

    }

    private static final JsonParser jsonParser=new JsonParser();
    private static String extracIDFromTweet(String tweetValue) {

        return jsonParser.parse(tweetValue)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    private static void readConfigValues() throws FileNotFoundException {
        Properties prop = new Properties();
        String propFileName = "config.properties";

        inputStream = ElasticSearchConsumer.class.getClassLoader().getResourceAsStream(propFileName);

        if (inputStream != null) {
            try {
                prop.load(inputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }
        hostname=prop.getProperty("hostname");
        username=prop.getProperty("username");
        password=prop.getProperty("password");
    }

    public static KafkaConsumer<String ,String> createConsumer(String topic){

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticSearch";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");
        KafkaConsumer<String ,String>consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
}

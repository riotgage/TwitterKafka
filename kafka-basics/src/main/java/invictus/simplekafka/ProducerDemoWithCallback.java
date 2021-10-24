package invictus.simplekafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        String bootstrapServer="127.0.0.1:9092";

        Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        // Set Config properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String,String>producer=new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++){

            ProducerRecord<String,String>record=new ProducerRecord<>("first_topic","hello world"+i);
            // send data : async
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Execute every time data sent or exception
                    if(e==null){
                        logger.info("\nReceived new Metadata\n"+
                                "Topic: "+recordMetadata.topic()+
                                "\nPartition: "+recordMetadata.partition()+
                                "\nOffset: "+recordMetadata.offset()+
                                "\nTimeStamp: "+recordMetadata.timestamp());
                    }else{
                        logger.error(e.getLocalizedMessage());
                    }
                }
            });
        }
        // Create A producer record


        // flush data
        producer.flush();
        producer.close();
    }
}

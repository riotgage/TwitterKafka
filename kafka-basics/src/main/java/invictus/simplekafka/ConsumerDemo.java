package invictus.simplekafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemo {
    public static void main(String[] args) {

        new ConsumerDemo().run();
    }

    public ConsumerDemo() {

    }

    public void run()  {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "first-application";
        String topic = "first_topic";
        // Set properties

        CountDownLatch latch = new CountDownLatch(1);


        ConsumerThread consumerThread = new ConsumerThread(
                bootstrapServers,
                groupId,
                topic,
                latch
        );

        Thread myThread=new Thread(consumerThread);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught ShutDown Hook");
            consumerThread.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application exited ");
        }));

        try {

            // sleep this thread until latch is non-zero
            // when latch becomes zero awake this thread or when there is any interruption
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted");
        }finally {
            logger.info("application exited");
        }
    }

    public static class ConsumerThread implements Runnable {

        private CountDownLatch latch;

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", value: " + record.value());
                        logger.info("Partition: " + record.partition() + " offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shutdown() {
            // throws WakeUpException
            consumer.wakeup();
        }
    }
}

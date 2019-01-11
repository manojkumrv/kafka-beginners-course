package kafka.tutorial1;

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

public class ConsumerDemoWithThread {

    public static void main(String[] args) {

        new ConsumerDemoWithThread().run();

    }

    public ConsumerDemoWithThread() {

    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        String kafkaAddress = "127.0.0.1:9092";
        String topic = "first-topic";
        String groupId = "my-sixth-application";
        CountDownLatch latch = new CountDownLatch(1);

        logger.info("Creating new consumer");
        Runnable consumerRunnable = new ConsumerRunnable(topic,kafkaAddress,groupId, latch);

        Thread myConsumerRunnableThread = new Thread(consumerRunnable);
        myConsumerRunnableThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable)consumerRunnable).shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application is interrupted: ", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String topic, String kafkaAddress, String groupId, CountDownLatch latch) {
            this.latch = latch;

            //Setting the properties for consumer
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //Creating a consumer
            consumer = new KafkaConsumer<String, String>(properties);

            //Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                //Consume the topic - Results are not printed till it's consumed
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + " Value: " + record.value());
                        logger.info("Partition: " + record.partition());
                        logger.info("Offsets: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received Shutdown Signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup(); //special method to interrupt .poll(). It will throw a wakeupexception
        }
    }
}


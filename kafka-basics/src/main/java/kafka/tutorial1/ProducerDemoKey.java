package com.github.manoj.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKey {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKey.class);
        String kafkaServerAddress = "127.0.0.1:9092";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerAddress);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            String topic = "first-topic";
            String message = "Hello World Again!" + i;
            String key = "id_" + i;

            System.out.println("Key: " + key);
            //Create a record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);

            //send data - asynchronus
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //Executes every time we send a data
                    if (e == null) {
                        //Executes when there is no exception
                        logger.info("Received new Metadata: \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offsets : " + recordMetadata.offset() + "\n" +
                                "Time : " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing data : ", e);
                    }
                }
            }).get(); //.get() is to make .send() synchronus - Do not repeat in real world applications
        }

        //flush
        producer.flush();
        //flush and close
        producer.close();
    }
}

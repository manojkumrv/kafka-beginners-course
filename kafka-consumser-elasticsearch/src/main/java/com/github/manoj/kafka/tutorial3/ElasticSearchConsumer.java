package com.github.manoj.kafka.tutorial3;

import com.google.gson.JsonParser;
import com.sun.xml.internal.ws.api.pipe.ContentType;
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
import org.elasticsearch.client.*;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static RestHighLevelClient createClient() {

        String hostName = "kafka-course-1957122210.ap-southeast-2.bonsaisearch.net";
        String userName = "fh4tgpnfqv";
        String password = "4h4ih4p7zz";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String kafkaAddress = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
        //Setting the properties for consumer

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");


        //Creating a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        //Consume the topic - Results are not printed till it's consumed

        BulkRequest bulkRequest = new BulkRequest();

        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records");
            for(ConsumerRecord<String, String> record : records) {

                //2 strategies are there for getting ID
                //Kafka generic ID
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                String id = extractIdfromTweet(record.value());
                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweets",
                        id
                ).source(record.value(), XContentType.JSON);

                bulkRequest.add(indexRequest);

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (recordCount > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing consumer offsets...");
                consumer.commitSync();
                logger.info("Offsets committed");
            }

        }
       // client.close();

    }

    private static JsonParser parser = new JsonParser();
    private static String extractIdfromTweet (String tweetJson) {

        return parser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}

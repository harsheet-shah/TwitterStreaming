package com.twitterstreaming;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class TwitterConsumer {
    private static final JsonParser jsonParser = new JsonParser();

    public static RestHighLevelClient createClient() {
        //https://ofaqtgk8fr:4v0dsoayf@kafka-twitter-stream-9291587402.us-east-1.bonsaisearch.net:443

        String hostName = "kafka-twitter-stream-9291587402.us-east-1.bonsaisearch.net";
        String userName = "ofaqtgk8fr";
        String password = "4v0dsoayf";
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
        return client;

    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        Logger logger = LoggerFactory.getLogger(TwitterConsumer.class);

        Properties properties = new Properties();
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "Cloud-Consumer";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//Manually committing the consumer offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "700");
        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe consumer
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(TwitterConsumer.class);
        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        //poll for new data
        while (true) {
            //System.out.println("hey");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            Integer recordCount = records.count();
            logger.info("Recieved " + records.count() + " records");
            BulkRequest bulkRequest = new BulkRequest();
            records.forEach(record -> {
                //inserting in the elastic search - bonsai elastic search


                try {
                    // 2 ways of generating ids
                /*
                1st generic way using kafka */
                    //String id =record.topic() +"-" + record.partition() + "-" + record.offset() ;

                    //2nd way we have ids from twitter data termed as id_str;
                    String id = extractJson(record.value());

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id //to make consumer idempotent - no more duplicates in elastic search
                            //everytime u run u will find the same sequence of ids..
                    ).source(record.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest);
                } catch (NullPointerException e) {
                    logger.warn("Skipping bad data- " + record.value());
                }

            });

            if (recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                consumer.commitSync();
                logger.info("Consumer Commit done");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static String extractJson(String record) {
        //google json extractor - gson
        return jsonParser.parse(record)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

}

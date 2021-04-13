package com.twitterstreaming;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class TwitterStreamFilter {
    private static final JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-stream");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> input_topic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filtered = input_topic.filter(
                (k, jsonTweet) -> extractUserFollower(jsonTweet) > 10000
        );
        filtered.to("popularUser");

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        kafkaStreams.start();
    }

    private static Integer extractUserFollower(String jsonTweet) {

        //gson library
        try {
            return jsonParser.parse(jsonTweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}

package com.twitterstreaming;

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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

//https://github.com/twitter/hbc
public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    String consumerKey = "JpkKxRo50gyRn7EdUd7P6a5N1";
    String consumerSecretKey = "Ae7AggVTzzofBy2uPzEqrGyCPCadBdNbv5HBhcMVBAXs50fuSt";
    String token = "723854148840595456-AYCD0xUN65qt2nzhMkjgISm9gQkIFXJ";
    String tokenSecret = "UDR8KVpelHxQCJCZVVKr8Oku85pbmkDvHuUgCPN29p3vv";

    List<String> terms = Lists.newArrayList("cloud", "azure", "amazonS3", "gcp");

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        logger.info("Starting Application");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        //creating Twitter client
        Client client = createClient(msgQueue);
        client.connect();

        //create KafkaProducer
        KafkaProducer<String, String> producer = createKafkaProducer();


        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down client");
            client.stop();
            logger.info("Shutting down producer");
            producer.close();
            logger.info("Bye.........");
        }));
        //loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.info("Error occured:" + e);
                        }
                    }
                });
            }


        }
        logger.info("End of Application");

    }

    public KafkaProducer<String, String> createKafkaProducer() {

        String bootstrapServers = "127.0.0.1:9092";
        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create safe produce
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        //high throughput settings
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;

    }

    public Client createClient(BlockingQueue<String> msgQueue) {


        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms);
        Authentication hosebirdAuth = new
                OAuth1(consumerKey, consumerSecretKey, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}

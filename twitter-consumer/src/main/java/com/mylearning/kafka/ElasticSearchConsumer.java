package com.mylearning.kafka;

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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static void main(String[] args) throws IOException {

        Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class);

        RestHighLevelClient client = createELKClient();

        KafkaConsumer<String, String> consumer = createKafkaConsumer("twitter_tweets");

        while (true) {
            //poll data from topic
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("Key {}, value {}", record.key(), record.value());
                LOGGER.info("Partition {}, offset {}", record.partition(), record.offset());

                String id = extractIdFromTweet(record.value());

                IndexRequest indexRequest = new IndexRequest("twitter1").id(id)
                        .source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                LOGGER.info(indexResponse.getId());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();
    }

    public static RestHighLevelClient createELKClient() {

        final String host = "";
        final String username = "";
        final String pwd = "";
        final int port = 443;

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, pwd));

        RestClientBuilder restClientBuilder =  RestClient.builder(
            new HttpHost(host, port, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        return new RestHighLevelClient(restClientBuilder);
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(String topic) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "elasticsearch-consumer-group";

        //create properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //String topic = "first_topic";

        //subscribe to topic
        consumer.subscribe(Collections.singleton(topic));
        return consumer;
    }

    public static String extractIdFromTweet(String tweetJson) {
        return JsonParser.parseString(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }
}

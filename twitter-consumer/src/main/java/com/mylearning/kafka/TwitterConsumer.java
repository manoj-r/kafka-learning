package com.mylearning.kafka;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

public class TwitterConsumer {

    public static void main(String[] args) {

    }

    public void createELKClient() {

        final String host = "kafka-learning-2808029324.us-west-2.bonsaisearch.net";
        final String username = "";
        final String pwd = "";
        final int port = 443;
        RestClientBuilder restClientBuilder =  RestClient.builder(
            new HttpHost(host, port, "https"));

        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
    }
}

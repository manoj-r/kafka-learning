# kafka-learning
A repository for all learning and tutorials on Kafka using reference materials from Stephen Mareek.


# Project
    ```
    Twitter -> Twitter_Hook -> Kafka -> Consumer -> Elastic search
    ```

# Commands

* Start Zookeeper

    ```shell script
    zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
    ```
  
* Start Kafka

    ```shell script
    kafka-server-start /usr/local/etc/kafka/server.properties
    ```  

* Create a topic

    ```shell script
    kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic twitter_tweets --partitions 6 --replication-factor 1
    ```

* Run a console producer

    ```shell script
    kafka-console-producer --broker-list 127.0.0.1:9092 --topic twitter_tweets  
    ```

* Run a console consumer

    ```shell script
    kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets
    ```

* List topics

    ```shell script
    kafka-topics --bootstrap-server 127.0.0.1:9092 --list
    ```

* List consumer group

    ```shell script
    kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --list
    ```
  
* Reset Offsets

    ```shell script
    kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --group elasticsearch-consumer-group --reset-offsets --topic twitter_tweets --execute --to-earliest
    ```  

# References

* https://github.com/simplesteph

* https://developer.twitter.com/en/docs/tweets/filter-realtime/overview

* https://github.com/twitter/hbc

* https://medium.com/@Ankitthakur/apache-kafka-installation-on-mac-using-homebrew-a367cdefd273

* https://kafka.apache.org/documentation



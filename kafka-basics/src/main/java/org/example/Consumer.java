package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("I am a Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo-java";
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        //create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        //poll a topic
        while(true){
            log.info("Polling! ");

            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record : records){

                log.info("key: " + record.key() + " value: " + record.value());
                log.info("Partition: " + record.partition() + " offsets: " + record.offset());
            }

        }

    }
}

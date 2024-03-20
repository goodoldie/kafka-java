package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a Kafka Producer");
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo-topic","hello world!");

        //send data (asynchronously)
        producer.send(producerRecord);

        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}

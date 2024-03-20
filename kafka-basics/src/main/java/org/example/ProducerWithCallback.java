package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallback.class.getSimpleName());
    public static void main(String[] args) throws InterruptedException {
        log.info("I am a Kafka Producer");
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //Not recommended for production
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i <10; i++) {

            //create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo-topic","hello world! " + i);

            //send data (asynchronously)
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //executes every time a producer publishes data successfully else throws exception
                    if(exception == null)
                        log.info("Received new metadata \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offsets: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    else
                        log.info("Error while producing", exception);
                }
            });
        }


        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}

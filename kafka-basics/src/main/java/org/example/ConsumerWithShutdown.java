package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWithShutdown.class.getSimpleName());

    private static KafkaConsumer<String, String> getStringStringKafkaConsumer(String groupId) {
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        //return a consumer
        return new KafkaConsumer<>(properties);
    }

    public static void main(String[] args) throws InterruptedException {
        log.info("I am a Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo-java";
        KafkaConsumer<String, String> consumer = getStringStringKafkaConsumer(groupId);

        //get a reference to the main thread
        final Thread mainThread = Thread.currentThread();
        
        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run(){
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();
                
                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        
        try {
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            //poll a topic
            while(true){

                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record : records){

                    log.info("key: " + record.key() + " value: " + record.value());
                    log.info("Partition: " + record.partition() + " offsets: " + record.offset());
                }

            }
        } catch (WakeupException e){
            log.info("Consumer is starting to shut down");
        } catch (Exception e){
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); //close the consumer, this will also commit offsets
            log.info("The consumer is now gracefully shutting down");
        }

    }


}

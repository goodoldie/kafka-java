package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithKeys.class.getSimpleName());
    public static void main(String[] args) throws InterruptedException {
        log.info("I am a Kafka Producer");
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j <2 ; j++) {
            for (int i = 0; i <10; i++) {

                String topic = "demo-java";
                String key = "id_" + i;
                String value = "Hello World " + i;

                //create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                //send data (asynchronously)
                try {
                    producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            //executes every time a producer publishes data successfully else throws exception
                            if(exception == null)
                                log.info("Key: " + key +
                                        " | Partition: " + metadata.partition() +
                                        " | Offset: " + metadata.offset());
                            else
                                log.info("Error while producing", exception);
                        }
                    });
                } catch (SerializationException e) {
                    log.info("Serialization Exception: ", e);
                } catch (BufferExhaustedException e){
                    log.info("Buffer Exhausted");
                }
                catch (TimeoutException e){
                    log.info("Buffer is full, timeout...");
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}

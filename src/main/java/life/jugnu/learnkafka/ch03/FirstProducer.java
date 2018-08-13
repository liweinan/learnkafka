package life.jugnu.learnkafka.ch03;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

class FirstProducer {
    public static void main(String[] args) {

        Properties p = new Properties();

        // Declare the propeties of cluster and informationa about data key and value
        p.put("bootstrap.servers", "localhost:9092");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("partitioner.class", "life.jugnu.learnkafka.ch03.LogEnabledPartitioner");

        // Create producer and send data in format : (topic name , key , value)
        Producer<String, String> pd = new KafkaProducer<>(p);
        ProducerRecord<String, String> rec = new ProducerRecord<>("firsttopic", "key", "value");

        // Kafka has 3 methods of sending
        // 1) Fire and forget
        pd.send(rec);

        ProducerRecord<String, String> rec2 = new ProducerRecord<>("firsttopic", "foo", "bar");

        // 2) Syncronous send , wait for response object
        try {
            pd.send(rec2).get();
        } catch (InterruptedException e) {
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        // 3 Asynchronous send , give a call back function and track success using call back
        ProducerRecord<String, String> rec3 = new ProducerRecord<>("firsttopic", "hello", "world");
        pd.send(rec3, new MyCallback());
        pd.close();
    }
}
package cn.zmhappy.stream;

import cn.zmhappy.config.BaseConfig;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.Scanner;

public class StreamInput {

    public static String processRecordMetadata(RecordMetadata recordMetadata) {

        return "[RecordMetadata]" + " offset = " + recordMetadata.offset() +
                " partition = " + recordMetadata.partition() +
                " timestamp = " + recordMetadata.timestamp() +
                " topic = " + recordMetadata.topic();

    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BaseConfig.getKafkaAddr());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        int count = 0;
        try {
            while (true) {
                count++;
                Scanner scan = new Scanner(System.in);
                String read = scan.nextLine();
                producer.send(new ProducerRecord<>("streams-plaintext-input", "" + Integer.toString(count), read),
                        (recordMetadata, e) -> System.out.println(processRecordMetadata(recordMetadata)));
                read = scan.nextLine();
                producer.send(new ProducerRecord<>("streams-plaintext-input-2", "" + Integer.toString(count), read),
                        (recordMetadata, e) -> System.out.println(processRecordMetadata(recordMetadata)));

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}

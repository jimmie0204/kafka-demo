package cn.zmhappy.kafka;

import cn.zmhappy.config.BaseConfig;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class ProducerMain {

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

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 50; i++) {
            producer.send(new ProducerRecord<>("my-topic-triple", "key-" + Integer.toString(i), "value-" + Integer.toString(i)), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println(processRecordMetadata(recordMetadata));
                }
            });
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        producer.close();
    }

}

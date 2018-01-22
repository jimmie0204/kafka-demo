package cn.zmhappy.stream;

import cn.zmhappy.config.BaseConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Arrays;
import java.util.Properties;

public class StreamOutput {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", BaseConfig.getKafkaAddr());
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("streams-pipe-output"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.println("offset = " + record.offset() + " key = " + record.key() + " value = " + record.value());
        }

    }
}

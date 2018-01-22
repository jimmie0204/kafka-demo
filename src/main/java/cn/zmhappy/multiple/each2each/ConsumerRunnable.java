package cn.zmhappy.multiple.each2each;

import cn.zmhappy.config.BaseConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerRunnable implements Runnable {

    private final KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BaseConfig.getKafkaAddr());
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("[" + Thread.currentThread().getName() + "] key = " + record.key() + " value = " + record.value()
                        + " topic = " + record.topic() + " partition = " + record.partition());            }
        }
    }
}

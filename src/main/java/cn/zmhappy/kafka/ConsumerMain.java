package cn.zmhappy.kafka;

import cn.zmhappy.config.BaseConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class ConsumerMain {

    private static String topic = "my-topic-triple";
//    private static String topic = "test";

    /**
     * 自动提交offset
     */
    public static void autoCommit() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BaseConfig.getKafkaAddr());
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

    /**
     * 手动提交offset
     */
    public static void manualCommit() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BaseConfig.getKafkaAddr());
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        final int minBatchSize = 5;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                insertIntoDb(buffer);
                consumer.commitSync();
                buffer.clear();
            }
        }

    }

    public static void insertIntoDb(List<ConsumerRecord<String, String>> buffer) {
        //TODO 插入数据库
        for (ConsumerRecord<String, String> consumerRecord : buffer) {
            System.out.println("[insert] key = " + consumerRecord.key() + " value = " + consumerRecord.value()
                    + " topic = " + consumerRecord.topic() + " partition = " + consumerRecord.partition());
        }
    }

    /**
     * 更精细地

     通过指定一个明确消息的偏移量为“已提交”
     */
    public static void manualCommitMoreClear() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BaseConfig.getKafkaAddr());
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));

        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("[get] key = " + record.key() + " value = " + record.value()
                                + " topic = " + record.topic() + " partition = " + record.partition());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }

    public static void suscribeOnSpecificPatitions() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BaseConfig.getKafkaAddr());
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        consumer.assign(Arrays.asList(partition0, partition1));

        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("[get] key = " + record.key() + " value = " + record.value()
                                + " topic = " + record.topic() + " partition = " + record.partition());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 手动指定分片的offset位置，将三个分片都从0开始读取
     */
    public static void manualOffset() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BaseConfig.getKafkaAddr());
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.assign(Arrays.asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2)));
        for (int i = 0; i < 3; i++) {
            consumer.seek(new TopicPartition(topic, i), 0);
        }
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println("[get] key = " + record.key() + " value = " + record.value()
                                + " topic = " + record.topic() + " partition = " + record.partition());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }

    }

    public static void main(String[] args) {
        autoCommit();
//        manualCommit();
//        manualCommitMoreClear();
//        suscribeOnSpecificPatitions();
//        manualOffset();
    }
}

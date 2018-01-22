package cn.zmhappy.kafka;

import cn.zmhappy.config.BaseConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;

/**
 * 管理client
 */
public class AdminClientMain {

    public static AdminClient getAdminClient() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BaseConfig.getKafkaAddr());
        AdminClient adminClient = AdminClient.create(props);
        return adminClient;
    }

    /**
     * 展示topic列表
     */
    public static void showTopics() {
        AdminClient adminClient = getAdminClient();
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        try {
            for (TopicListing topicListing : listTopicsResult.listings().get()) {
                System.out.println(topicListing.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        adminClient.close();
    }

    /**
     * 新建topic
     */
    public static void createTopics(String topicName, int numPartitions, int replicationFactor) {
        AdminClient adminClient = getAdminClient();
        adminClient.createTopics(Arrays.asList(new NewTopic(topicName, numPartitions, (short) replicationFactor)));
        adminClient.close();
    }

    /**
     * 删除topic
     */
    public static void deleteTopics(String topicName) {
        AdminClient adminClient = getAdminClient();
        adminClient.deleteTopics(Arrays.asList(topicName));
        adminClient.close();
    }

    /**
     * 获得具体topic的信息，包括name，internal，partitions
     * (name=my-topic-triple,
     * internal=false,
     * partitions=
     * (
     *      partition=0,
     *      leader=192.168.1.111:9094 (id: 2 rack: null),
     *      replicas=192.168.1.111:9094 (id: 2 rack: null), 192.168.1.111:9092 (id: 0 rack: null), 192.168.1.111:9093 (id: 1 rack: null),
     *      isr=192.168.1.111:9092 (id: 0 rack: null), 192.168.1.111:9093 (id: 1 rack: null), 192.168.1.111:9094 (id: 2 rack: null)
     * ),
     * (
     *      partition=1,
     *      leader=192.168.1.111:9094 (id: 2 rack: null),
     *      replicas=192.168.1.111:9092 (id: 0 rack: null), 192.168.1.111:9093 (id: 1 rack: null), 192.168.1.111:9094 (id: 2 rack: null),
     *      isr=192.168.1.111:9094 (id: 2 rack: null), 192.168.1.111:9093 (id: 1 rack: null), 192.168.1.111:9092 (id: 0 rack: null)
     * ),
     * (
     *      partition=2,
     *      leader=192.168.1.111:9093 (id: 1 rack: null),
     *      replicas=192.168.1.111:9093 (id: 1 rack: null), 192.168.1.111:9094 (id: 2 rack: null), 192.168.1.111:9092 (id: 0 rack: null),
     *      isr=192.168.1.111:9094 (id: 2 rack: null), 192.168.1.111:9093 (id: 1 rack: null), 192.168.1.111:9092 (id: 0 rack: null)
     * ))
     */
    public static void describeTopics(String topicName) {
        AdminClient adminClient = getAdminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(topicName));
        Map<String, KafkaFuture<TopicDescription>> values = describeTopicsResult.values();
        try {
            for (String key : values.keySet()) {
                System.out.println(values.get(key).get().toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        adminClient.close();
    }

    /**
     * 展示topic的config
     * Config(
     *      entries=[
     *          ConfigEntry(name=compression.type, value=producer, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=message.format.version, value=1.0-IV0, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=file.delete.delay.ms, value=60000, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=leader.replication.throttled.replicas, value=, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=max.message.bytes, value=1000012, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=min.compaction.lag.ms, value=0, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=message.timestamp.type, value=CreateTime, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=min.insync.replicas, value=1, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=segment.jitter.ms, value=0, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=preallocate, value=false, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=index.interval.bytes, value=4096, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=min.cleanable.dirty.ratio, value=0.5, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=unclean.leader.election.enable, value=false, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=retention.bytes, value=-1, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=delete.retention.ms, value=86400000, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=cleanup.policy, value=delete, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=flush.ms, value=9223372036854775807, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=follower.replication.throttled.replicas, value=, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=segment.bytes, value=1073741824, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=retention.ms, value=604800000, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=segment.ms, value=604800000, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=message.timestamp.difference.max.ms, value=9223372036854775807, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=flush.messages, value=9223372036854775807, isDefault=true, isSensitive=false, isReadOnly=false),
     *          ConfigEntry(name=segment.index.bytes, value=10485760, isDefault=true, isSensitive=false, isReadOnly=false)
     *      ]
     * )
     */
    public static void showConfig(String topicName) {
        AdminClient adminClient = getAdminClient();
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(new ConfigResource(ConfigResource.Type.TOPIC, topicName)));
        Map<ConfigResource, KafkaFuture<Config>> values = describeConfigsResult.values();
        try {
            for (ConfigResource configResource : values.keySet()) {
                System.out.println(values.get(configResource).get().toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 修改config，属性的name和默认值可以参考上面showConfig
     */
    public static void alterConfig(String topicName, String key, String value) {
        AdminClient adminClient = getAdminClient();
        Map<ConfigResource, Config>  config = new HashMap<>();
        config.put(new ConfigResource(ConfigResource.Type.TOPIC, topicName), new Config(Arrays.asList(new ConfigEntry(key, value))));
        adminClient.alterConfigs(config);
    }

    /**
     * 增加partitions到多少，不能比原来少
     * @param topicName
     * @param numPartitions
     */
    public static void createPartitions(String topicName, int numPartitions) {
        AdminClient adminClient = getAdminClient();
        Map<String, NewPartitions> map = new HashMap<>();
        map.put(topicName, NewPartitions.increaseTo(numPartitions));
        adminClient.createPartitions(map);
        adminClient.close();
    }

    public static void main(String[] args) {
//        deleteTopics("new_create_topic_1");
//        createTopics("new_create_topic_1", 3, 3);
//        showTopics();
//        describeTopics();
//        alterConfig();
        describeTopics("new_create_topic_1");
//        showConfig();
//        alterConfig();
//        showConfig();
        createPartitions("new_create_topic_1", 6);
        describeTopics("new_create_topic_1");

    }

}

package cn.zmhappy.config;


public class BaseConfig {

    private static final String zkAddr = "192.168.1.111:2181";

    private static final String kafkaAddr = "192.168.1.111:9092,192.168.1.111:9093,192.168.1.111:9094";
//    private static final String kafkaAddr = "192.168.1.125:9092";

    public static String getZkAddr() {
        return zkAddr;
    }

    public static String getKafkaAddr() {
        return kafkaAddr;
    }
}

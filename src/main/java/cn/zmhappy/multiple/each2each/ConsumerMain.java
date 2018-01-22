package cn.zmhappy.multiple.each2each;

public class ConsumerMain {

    public static void main(String[] args) {
        String topic = "my-topic-triple";
        int consumerNum = 3;

        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, topic);
        consumerGroup.execute();
    }
}

package cn.zmhappy.multiple.pool;

public class ConsumerMain {

    public static void main(String[] args) {
        String topic = "my-topic-triple";
        int workerNum = 6;

        ConsumerHandler consumerHandler = new ConsumerHandler(topic);
        consumerHandler.execute(workerNum);

        try {
            Thread.sleep(1000000);
        } catch (InterruptedException e) {
            consumerHandler.shutdown();
        }
    }
}

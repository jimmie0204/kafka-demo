package cn.zmhappy.multiple.each2each;

import java.util.ArrayList;
import java.util.List;

public class ConsumerGroup {

    private List<ConsumerRunnable> consumers;

    public ConsumerGroup(int consumerNum, String topic) {
        consumers = new ArrayList<>(consumerNum);
        for (int i = 0; i < consumerNum; i++) {
            ConsumerRunnable consumerThread = new ConsumerRunnable(topic);
            consumers.add(consumerThread);
        }
    }

    public void execute() {
        for (ConsumerRunnable task : consumers) {
            new Thread(task).start();
        }
    }

}

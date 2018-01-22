package cn.zmhappy.multiple.pool;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Worker implements Runnable {

    private ConsumerRecord<String, String> consumerRecord;

    public Worker(ConsumerRecord record) {
        this.consumerRecord = record;
    }

    @Override
    public void run() {
        System.out.println("[" + Thread.currentThread().getName() + "] key = " + consumerRecord.key() + " value = " + consumerRecord.value()
                + " topic = " + consumerRecord.topic() + " partition = " + consumerRecord.partition());
    }
}

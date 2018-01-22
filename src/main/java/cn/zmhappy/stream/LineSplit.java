package cn.zmhappy.stream;

import cn.zmhappy.config.BaseConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LineSplit {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BaseConfig.getKafkaAddr());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        /**
         * Source: KSTREAM-SOURCE-0000000000 (topics: [streams-plaintext-input])
         *      --> KSTREAM-FLATMAPVALUES-0000000001
         * Processor: KSTREAM-FLATMAPVALUES-0000000001 (stores: [])
         *      --> KSTREAM-SINK-0000000002
         *      <-- KSTREAM-SOURCE-0000000000
         * Sink: KSTREAM-SINK-0000000002 (topic: streams-pipe-output)
         *      <-- KSTREAM-FLATMAPVALUES-0000000001
         */
        builder.<String, String>stream("streams-plaintext-input")
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .to("streams-pipe-output");

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }

        System.exit(0);


    }
}

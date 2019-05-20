package com.mingm.jr.kafkastream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by jiamingming on 2017/12/25.
 */
@Service
public class BalanceStreams {

    private static final Logger logger = LoggerFactory.getLogger(BalanceStreams.class);




    @PostConstruct
    public void streams(){

        logger.info("service start...");
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-balance");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,MyEventTimeExtractor.class.getName());
        props.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG,10000);//获取窗口数据时间间隔 ms
        //props.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG,10000);

        props.put(ConsumerConfig.GROUP_ID_CONFIG,"stream-balance");
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");//消费kafka最新偏移量位置的数据
        //props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("streams-control-input", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, Long> source1 = source.map(
                new KeyValueMapper<String, String, KeyValue<String, Long>>() {
                    @Override
                    public KeyValue<String, Long> apply(String s, String s2) {
                        return new KeyValue<>(s, Long.valueOf(s2));
                    }
                }
        );

        // count , per 10 second
        final KGroupedStream<String, Long> stringStringKGroupedStream = source1.groupByKey();
        KTable<Windowed<String>,Long> astream = stringStringKGroupedStream.windowedBy(
                TimeWindows.of(TimeUnit.SECONDS.toMillis(30))
        ).aggregate(
                new Initializer<Long>() {
                    @Override
                    public Long apply() {//init
                        return 0L;
                    }
                },
                new Aggregator<String, Long, Long>() {//sum
                    @Override
                    public Long apply(String s, Long aLong, Long aLong2) {
                        //System.err.println(aLong + "===" + aLong2);
                        return aLong + aLong2;
                    }
                }
        );


        astream.foreach(
                new ForeachAction<Windowed<String>, Long>() {
                    @Override
                    public void apply(Windowed<String> stringWindowed, Long aLong) {

                        logger.info("stream result : "+stringWindowed.key()+" val: "+aLong);
                        //System.out.println(stringWindowed.key()+"---"+aLong);
                    }
                }
        );
        final CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-balance-service") {
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
            logger.error("err:{},",e.toString());
            System.err.println("err: " + e.toString());
            System.exit(1);
        }
        System.exit(0);
    }
}

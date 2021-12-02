package com.modeln.intg.client.SamplePocCode.MultiThreadSamples.MultithreadInitial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaMultiThreadConsumer {
    private static final int CONCURRENT_PARTITIONS_COUNT = 2;
    private static final ExecutorService executorService
            = Executors.newFixedThreadPool(CONCURRENT_PARTITIONS_COUNT);

    private static class ConsumerWorker implements Runnable{

        private final KafkaConsumer<String,String> consumer;

        public ConsumerWorker(Properties properties, String topic) {
            this.consumer = new KafkaConsumer(properties);
            consumer.subscribe(Collections.singletonList(topic));
        }

        public void run() {
            final String id = Thread.currentThread().getId()
                    +"-"+System.identityHashCode(consumer);
            try {
                while(true){
                    ConsumerRecords<String, String> records
                            = consumer.poll(Duration.ofMillis(500));
                    for(ConsumerRecord<String, String> record:records){
                        System.out.println("ID : "+id+" | "+String.format(
                                "Subject: %s, Partition: %d, Offset: %d," +
                                        "key：%s，value：%s",
                                record.topic(),record.partition(),
                                record.offset(),record.key(),record.value()));
                        //do our work
                    }
                }
            } finally {
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.0.117:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"concurrent");

        for(int i = 0; i<CONCURRENT_PARTITIONS_COUNT; i++){
            executorService.submit(new ConsumerWorker(properties,
                    "concurrent-test"));
        }
    }
}

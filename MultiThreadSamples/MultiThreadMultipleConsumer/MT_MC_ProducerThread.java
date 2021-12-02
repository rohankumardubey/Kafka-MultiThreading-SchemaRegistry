package com.modeln.intg.client.SamplePocCode.MultiThreadSamples.MultiThreadMultipleConsumer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MT_MC_ProducerThread implements Runnable {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public MT_MC_ProducerThread(String brokers, String topic) {
        Properties prop = createProducerConfig(brokers);
        this.producer = new KafkaProducer<String, String>(prop);
        this.topic = topic;
    }

    private static Properties createProducerConfig(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    @Override
    public void run() {
        System.out.println("Produces 4 messages");
        for (int i = 0; i < 5; i++) {
            String msg = "Message " + i;
            producer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    }
                    System.out.println("Sent:" + msg + ", Partition: " + metadata.partition() + ", Offset: "
                            + metadata.offset());
                }
            });

        }
        // closes producer
        producer.close();

    }
}
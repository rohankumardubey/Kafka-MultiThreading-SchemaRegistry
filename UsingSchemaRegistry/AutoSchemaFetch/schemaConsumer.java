package com.modeln.intg.client.SamplePocCode.UsingSchemaRegistry.AutoSchemaFetch;


//import com.modeln.intg.client.SamplePocCode.UsingSchemaRegistry;
import org.apache.kafka.clients.consumer.*;

import java.util.Collections;
import java.util.Properties;

@SuppressWarnings("InfiniteLoopStatement")
public class schemaConsumer {
    public static void main(String[] args) {
        final String topic = "SchemaTransactions";
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");


        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        final Consumer<String, Payment> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Collections.singletonList(topic));
//        try {
//            while (true) {
//                ConsumerRecords<String, Payment> records = consumer.poll(100);
//                for (ConsumerRecord<String, Payment> record : records) {
//                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
//                }
//            }
//        } finally {
//            consumer.close();
//        }

    }
}

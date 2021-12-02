package com.modeln.intg.client.SamplePocCode.RecordWithHeaders;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) throws Exception {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            props.put("schema.registry.url", "http://localhost:8081");

            HashMap<String,String> hdrs = new HashMap<>();
            hdrs.put("sample","header");
            envelope env =new envelope("enty","tnt","dest","src",hdrs);
            String destination=env.getDestination();
            String Entity= env.getEntity();
            String Tenant=env.getTenant();
            String Source = env.getSource();
            String Payload = "payload";
            String Topic="topic";


            ProducerRecordEnvelop producerRecordEnvelop = new ProducerRecordEnvelop(Entity,Tenant,Source,destination,hdrs,Payload);
            KafkaProducer<String, Object> producer = new KafkaProducer<>(props);


            ProducerRecord<String, Object> record = new ProducerRecord<>(Topic,null,System.currentTimeMillis(),"KeyRecordSend",producerRecordEnvelop);
            try {
//                for ( Map.Entry<String, String> headerRecords : header.entrySet()) {
//                    String HeaderKey = headerRecords.getKey();
//                    String HeaderValue = headerRecords.getValue();
//                    record.headers().add(HeaderKey, HeaderValue.getBytes(StandardCharsets.UTF_8));
                    producer.send(record);
//                    System.out.println(record);
                    System.out.println("message sent!!");
//                }

            } catch (SerializationException e) {
                System.out.println("!!error");
            }

            finally {
                producer.flush();
            }
            producer.close();

    }

}

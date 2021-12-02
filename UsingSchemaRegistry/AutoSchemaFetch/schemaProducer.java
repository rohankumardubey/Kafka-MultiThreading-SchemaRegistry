package com.modeln.intg.client.SamplePocCode.UsingSchemaRegistry.AutoSchemaFetch;


import com.modeln.intg.client.SamplePocCode.UsingSchemaRegistry.AutoSchemaFetch.Payment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import java.util.Properties;



public class schemaProducer {
   public static void main(String[] args) {
       final String Topic="SchemaTransactions";
       Properties props = new Properties();
       props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
       props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
               org.apache.kafka.common.serialization.StringSerializer.class);
       props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
               io.confluent.kafka.serializers.KafkaAvroSerializer.class);
       props.put("schema.registry.url", "http://localhost:8081");


       try(KafkaProducer<String, Payment> producer = new KafkaProducer<>(props)){
           for(long i=0;i<10;i++){
               final String OrderId = "id" + i;
               final double amount = (i*500);
               final Payment payment = new Payment(OrderId,amount,"region");
               final ProducerRecord<String,Payment> record = new ProducerRecord<>(Topic, payment.getId(), payment);
               producer.send(record);
               Thread.sleep(1000);
           }
           producer.flush();
           System.out.println("send all messages : "+Topic);
       }
       catch(final SerializationException | InterruptedException e ){
           e.printStackTrace();
       }
   }

}

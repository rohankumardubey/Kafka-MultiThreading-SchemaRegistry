package com.modeln.intg.client.SamplePocCode.UsingSchemaRegistry.RegisterFromSchemaRegistry;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import java.io.IOException;
import java.util.Properties;


public class SchemaRegistryProducer {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");


        KafkaProducer<String, Object> producer = new KafkaProducer<>(props);
        final String userSchema= SchemaMain.SchemaReturn();
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        final String Topic = SchemaMain.Topic;

            ProducerRecord<String, Object> record = new ProducerRecord<>(Topic, avroRecord);
            try {
                    avroRecord.put("id","hi");
                    avroRecord.put("amount",100.0d);
                    producer.send(record);
            } catch (SerializationException e) {
        // may need add fault tolerent error handiling here
                System.out.println("!!error");
            }

            finally {
        // When you're finished producing records,
        // we can flush the producer to ensure it has all been written to Kafka
                producer.flush();
            }

        // close the producer to free its resources.
        producer.close();

    }

}

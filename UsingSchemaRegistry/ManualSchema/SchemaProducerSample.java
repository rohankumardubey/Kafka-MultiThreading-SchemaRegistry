package com.modeln.intg.client.SamplePocCode.UsingSchemaRegistry.ManualSchema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class SchemaProducerSample {

    public static void main(String[] args) {

        RandomStringGeneration rand = new RandomStringGeneration();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);

        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);

        //creating sample Stringg values as values
        List<Object> user = new ArrayList<Object>();

        for (int i=0;i<=10;i++){
            user.add(rand.generate());
        }

        //creating sample keys
        List<String> keys = new ArrayList<String>();
        for (int i=0;i<=10;i++){
            keys.add(rand.generate()+"_"+i);
        }

        for (String j : keys) {

            ProducerRecord<Object, Object> record = new ProducerRecord<>("SchemaTopicUsage", j, avroRecord);
            try {

                for (Object i : user) {
                    avroRecord.put("name", i);
                    producer.send(record);
                }

            } catch (SerializationException e) {
                // may need to do something with it
                System.out.println("!!error");
            }

            finally {
                // When you're finished producing records,
                // we can flush the producer to ensure it has all been written to Kafka
                producer.flush();
            }
        }
// close the producer to free its resources.
        producer.close();

    }
}

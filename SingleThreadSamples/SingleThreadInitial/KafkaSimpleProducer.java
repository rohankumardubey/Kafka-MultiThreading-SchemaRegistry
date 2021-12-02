package com.modeln.intg.client.SamplePocCode.SingleThreadSamples.SingleThreadInitial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaSimpleProducer {

    public static void main(String[] args) {

        //To connect to kafka we need to create Properties object
        //in properties we need to specify three properties
        //1)Bootstrap server
        //2)key serializer
        //3)value serializer
        //Setup Properties for Kafka Producer
        Properties kafkaProps = new Properties();

        //List of brokers to connect to
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                            "localhost:9092");

        //Serializer class used to convert Keys to Byte Arrays
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                            "org.apache.kafka.common.serialization.StringSerializer");

        //Serializer class used to convert Messages to Byte Arrays
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                            "org.apache.kafka.common.serialization.StringSerializer");

        //Create a Kafka producer object from configuration properties
        KafkaProducer<String,String> simpleProducer = new KafkaProducer<String,String>(kafkaProps);

        //creating a random key
//            int startKey = (new Random()).nextInt(1000) ;

            for( int i=0; i <  10; i++) {

                //Create a producer Record to insert into the kafka producer
                ProducerRecord<String, String> kafkaRecord =
                        new ProducerRecord<String, String>(
                                "kafka.learning.orders",    //Topic name
                                String.valueOf(i),               //Key for the message
                                "This is order : " + i         //value of Message Content
                        );

                System.out.println("Sending Message : " + kafkaRecord.toString());

                //Publish to Kafka
                simpleProducer.send(kafkaRecord);
            }
            //closing the conection for the producer in kafka
            simpleProducer.close();

    }
}

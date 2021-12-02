package com.modeln.intg.client.SamplePocCode.SingleThreadSamples.SingleThreadFinal;


import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


//Create java class named “SimpleProducer”
public class SimpleProducer {

    public static void main(String[] args) throws Exception{

        // Check arguments length value
//        if(args.length == 0){
//            System.out.println("Enter topic name");
//            return;
//        }

        //Assign topicName to string variable
        String topicName ="sample";

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
//        props.put("bootstrap.servers", “192.168.0.117:9092");

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        //Set acknowledgements for producer requests.
//        props.put("acks", “all");

                //If the request fails, the producer can automatically retry,
                props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("schema.registry.url", "http://0.0.0.0:8081");



        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);



        for( int i=0; i <  100; i++) {

            //Create a producer Record to insert into the kafka producer
            ProducerRecord<String, String> kafkaRecord =
                    new ProducerRecord<String, String>(
                            topicName,    //Topic name
                            String.valueOf(i),               //Key for the message
                            "This is order : " + i         //value of Message Content
                    );

            System.out.println("Sending Message : " + kafkaRecord.toString());

            //Publish to Kafka
            producer.send(kafkaRecord);
        }

        System.out.println("Message sent successfully");
        producer.close();
    }
}
package com.modeln.intg.client.SamplePocCode.MultiThreadSamples.MultithreadInitial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaMultiThreadProducer {

    //Number of messages sent
    private static final int MSG_SIZE = 1000;
    //The thread pool responsible for sending messages
    private static final ExecutorService executorService
            = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors());
    private static final CountDownLatch countDownLatch
            = new CountDownLatch(MSG_SIZE);

    private static DemoUser makeUser(int id){
        DemoUser demoUser = new DemoUser(id);
        String userName = "test_"+id;
        demoUser.setName(userName);
        return demoUser;
    }

    /*Send message task*/
    private static class ProduceWorker implements Runnable{

        private final ProducerRecord<String,String> record;
        private final KafkaProducer<String,String> producer;

        public ProduceWorker(ProducerRecord<String, String> record,
                             KafkaProducer<String, String> producer) {
            this.record = record;
            this.producer = producer;
        }

        public void run() {
            final String id = Thread.currentThread().getId()
                    +"-"+System.identityHashCode(producer);
            try {
                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata metadata,
                                             Exception exception) {
                        if(null!=exception){
                            exception.printStackTrace();
                        }
                        if(null!=metadata){
                            System.out.println("ID: "+id+"|"
                                    +String.format("Offset: %s, Partition: %s",
                                    metadata.offset(),metadata.partition()));
                        }
                    }
                });
                System.out.println("ID: "+id+" :data["+record+"]Has been sent.");
                countDownLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.0.117:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer(
                properties)) {
            //Send cyclically, through the thread pool
            for (int i = 0; i < MSG_SIZE; i++) {
                DemoUser demoUser = makeUser(i);
                ProducerRecord<String, String> record
                        = new ProducerRecord(
                        "concurrent-test", null,
                        System.currentTimeMillis(),
                        demoUser.getId() + "", demoUser.toString());
                executorService.submit(new ProduceWorker(record, producer));
            }
            countDownLatch.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
    }
}


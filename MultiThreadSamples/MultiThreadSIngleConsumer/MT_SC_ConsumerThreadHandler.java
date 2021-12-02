package com.modeln.intg.client.SamplePocCode.MultiThreadSamples.MultiThreadSIngleConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class MT_SC_ConsumerThreadHandler implements Runnable {

  private ConsumerRecord consumerRecord;

  public MT_SC_ConsumerThreadHandler(ConsumerRecord consumerRecord) {
    this.consumerRecord = consumerRecord;
  }

  public void run() {
    System.out.println("Process: " + consumerRecord.value() + ", Offset: " + consumerRecord.offset()
        + ", By ThreadID: " + Thread.currentThread().getId());
  }
}
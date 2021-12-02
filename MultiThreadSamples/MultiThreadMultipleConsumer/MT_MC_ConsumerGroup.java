package com.modeln.intg.client.SamplePocCode.MultiThreadSamples.MultiThreadMultipleConsumer;

import java.util.ArrayList;
import java.util.List;

public class MT_MC_ConsumerGroup {

    private final int numberOfConsumers;
    private final String groupId;
    private final String topic;
    private final String brokers;
    private List<MT_MC_ConsumerThread> consumers;

    public MT_MC_ConsumerGroup(String brokers, String groupId, String topic,
                                     int numberOfConsumers) {
        this.brokers = brokers;
        this.topic = topic;
        this.groupId = groupId;
        this.numberOfConsumers = numberOfConsumers;
        consumers = new ArrayList<>();
        for (int i = 0; i < this.numberOfConsumers; i++) {
            MT_MC_ConsumerThread ncThread =
                    new MT_MC_ConsumerThread(this.brokers, this.groupId, this.topic);
            consumers.add(ncThread);
        }
    }

    public void execute() {
        for (MT_MC_ConsumerThread ncThread : consumers) {
            Thread t = new Thread(ncThread);
            t.start();
        }
    }

    /**
     * @return the numberOfConsumers
     */
    public int getNumberOfConsumers() {
        return numberOfConsumers;
    }

    /**
     * @return the groupId
     */
    public String getGroupId() {
        return groupId;
    }

}
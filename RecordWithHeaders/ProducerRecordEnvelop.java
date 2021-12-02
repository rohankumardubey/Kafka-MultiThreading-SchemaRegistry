package com.modeln.intg.client.SamplePocCode.RecordWithHeaders;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;

/**
 * Message to be sent to the intg service message bus.
 */
public class ProducerRecordEnvelop {

    private final envelope envelope;
    private final String payload;



    public ProducerRecordEnvelop (String enty, String tnt, String dest, String src, HashMap<String, String> hdrs,String payload){
        this.payload = payload;
        this.envelope = createEnvelope(enty,tnt,dest,src,hdrs);
        if(StringUtils.isBlank(envelope.getEntity()) || StringUtils.isBlank(envelope.getTenant()) || StringUtils.isBlank(envelope.getDestination()) || StringUtils.isBlank(envelope.getSource())){
            throw new IllegalArgumentException("Attributes cannot be blank.");
        }

    }

    public String getEntity(){
        return envelope.getEntity();
    }

    public String getTenant(){
        return envelope.getTenant();
    }

    public String getDestination(){
        return envelope.getDestination();
    }

    public String getSource(){
        return envelope.getSource();
    }


    public Object getPayload() {
        return payload;
    }

    //for internal use, not to be exposed to apps
    envelope getEnvelope() {
        return envelope;
    }

    private envelope createEnvelope(String enty, String tnt, String dest, String src, HashMap<String, String> hdrs){
        //Note: schemaVersion is set by CMnSerDe during serialization based on latest schema
        return new envelope(enty,tnt,dest,src,hdrs);
    }


}

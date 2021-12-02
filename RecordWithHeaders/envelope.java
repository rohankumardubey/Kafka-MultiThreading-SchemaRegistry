package com.modeln.intg.client.SamplePocCode.RecordWithHeaders;


import org.apache.commons.lang3.StringUtils;
import java.util.HashMap;

/**
 * Integration Service Message Envelope
 */
public final class envelope {

    private HashMap<String, String> headers = new HashMap<String, String>();
    private String entity;
    private String tenant;
    private String destination;
    private String source;

    public envelope(String enty, String tnt, String dest, String src, HashMap<String, String> hdrs){
        setEntity(enty);
        setTenant(tnt);
        setDestination(dest);
        setSource(src);
        setHeaders(hdrs);
    }

    /**
     * @return entity
     */
    public String getEntity() {
        return this.entity;
    }

    private void setEntity(String enty){
        if(StringUtils.isBlank(enty)){
            throw new IllegalArgumentException("Entity cannot be blank");
        }
        this.entity = enty;
    }

    /**
     * @return tenant id
     */
    public String getTenant() {
        return tenant;
    }

    private void setTenant(String tenant){
        if(StringUtils.isBlank(tenant)){
            throw new IllegalArgumentException("Tenant cannot be blank");
        }
        this.tenant = tenant;
    }

    /**
     * @return destination
     */
    public String getDestination() {
        return this.destination;
    }

    private void setDestination(String dest) {
        if(StringUtils.isBlank(dest)){
            throw new IllegalArgumentException("Destination cannot be blank");
        }
        this.destination = dest;
    }

    /**
     * @return source of the message
     */
    public String getSource() {
        return source;
    }

    private void setSource(String source) {
        if(StringUtils.isBlank(source)){
            throw new IllegalArgumentException("Message Source cannot be blank");
        }
        this.source = source;
    }

    /**
     * @return header value
     */
    public String getHeader(String key) {
        if (!this.headers.containsKey(key)) {
            return null;
        }
        return this.headers.get(key);
    }


    private void setHeaders(HashMap<String, String> hdrs) {
        for (String key : hdrs.keySet()) {
            this.headers.put(key, hdrs.get(key));
        }
        this.source = source;
    }





}

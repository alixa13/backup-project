package com.example.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Common model class for Zeek log events.
 * This represents the core data structure for log events processed by the Flink jobs.
 */
public class ZeekLogEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    
    // Common fields for all Zeek logs
    private String logType;     // The type of log (conn, dns, http, ssl)
    private String ts;          // Timestamp of the event
    private String uid;         // Unique ID for the connection
    private String sourceIp;    // Source IP address
    private String destIp;      // Destination IP address
    private int sourcePort;     // Source port
    private int destPort;       // Destination port
    private String proto;       // Protocol (tcp, udp, icmp)
    
    // Optional fields (may not be present in all log types)
    private String service;     // Service type if identified
    private double duration;    // Connection duration
    private long origBytes;     // Bytes sent by originator
    private long respBytes;     // Bytes sent by responder
    private String connState;   // Connection state
    
    // Analysis and enrichment fields
    private String prediction;  // Classification results
    private double anomalyScore; // Anomaly detection score
    
    // Map for additional fields specific to each log type
    private Map<String, Object> additionalFields = new HashMap<>();
    
    /**
     * Default constructor
     */
    public ZeekLogEvent() {
    }
    
    /**
     * Constructor with log type
     */
    public ZeekLogEvent(String logType) {
        this.logType = logType;
    }
    
    /**
     * Converts this event to a Map for JSON serialization
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        
        // Add common fields if not null
        if (logType != null) map.put("log_type", logType);
        if (ts != null) map.put("ts", ts);
        if (uid != null) map.put("uid", uid);
        if (sourceIp != null) map.put("src_ip", sourceIp);
        if (destIp != null) map.put("dest_ip", destIp);
        if (sourcePort > 0) map.put("src_port", sourcePort);
        if (destPort > 0) map.put("dest_port", destPort);
        if (proto != null) map.put("proto", proto);
        
        // Add optional fields if set
        if (service != null) map.put("service", service);
        if (duration > 0) map.put("duration", duration);
        if (origBytes > 0) map.put("orig_bytes", origBytes);
        if (respBytes > 0) map.put("resp_bytes", respBytes);
        if (connState != null) map.put("conn_state", connState);
        
        // Add analysis fields if set
        if (prediction != null) map.put("prediction", prediction);
        if (anomalyScore > 0) map.put("anomaly_score", anomalyScore);
        
        // Add all additional fields
        map.putAll(additionalFields);
        
        return map;
    }
    
    /**
     * Add a custom field to the event
     */
    public void addField(String key, Object value) {
        if (value != null) {
            additionalFields.put(key, value);
        }
    }
    
    // Getters and setters
    
    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp;
    }

    public String getDestIp() {
        return destIp;
    }

    public void setDestIp(String destIp) {
        this.destIp = destIp;
    }

    public int getSourcePort() {
        return sourcePort;
    }

    public void setSourcePort(int sourcePort) {
        this.sourcePort = sourcePort;
    }

    public int getDestPort() {
        return destPort;
    }

    public void setDestPort(int destPort) {
        this.destPort = destPort;
    }

    public String getProto() {
        return proto;
    }

    public void setProto(String proto) {
        this.proto = proto;
    }

    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    public double getDuration() {
        return duration;
    }

    public void setDuration(double duration) {
        this.duration = duration;
    }

    public long getOrigBytes() {
        return origBytes;
    }

    public void setOrigBytes(long origBytes) {
        this.origBytes = origBytes;
    }

    public long getRespBytes() {
        return respBytes;
    }

    public void setRespBytes(long respBytes) {
        this.respBytes = respBytes;
    }

    public String getConnState() {
        return connState;
    }

    public void setConnState(String connState) {
        this.connState = connState;
    }

    public String getPrediction() {
        return prediction;
    }

    public void setPrediction(String prediction) {
        this.prediction = prediction;
    }

    public double getAnomalyScore() {
        return anomalyScore;
    }

    public void setAnomalyScore(double anomalyScore) {
        this.anomalyScore = anomalyScore;
    }

    public Map<String, Object> getAdditionalFields() {
        return additionalFields;
    }

    public void setAdditionalFields(Map<String, Object> additionalFields) {
        this.additionalFields = additionalFields;
    }
} 
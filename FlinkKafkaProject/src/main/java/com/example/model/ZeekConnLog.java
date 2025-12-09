package com.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Model class for Zeek Connection log events.
 * Represents the structure of connection log entries from Zeek.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ZeekConnLog {
    // Common fields
    @JsonProperty("ts")
    private String ts;
    
    @JsonProperty("uid")
    private String uid;
    
    @JsonProperty("id.orig_h")
    private String idOrigH;
    
    @JsonProperty("id.orig_p")
    private Integer idOrigP;
    
    @JsonProperty("id.resp_h")
    private String idRespH;
    
    @JsonProperty("id.resp_p")
    private Integer idRespP;
    
    // Connection specific fields
    @JsonProperty("proto")
    private String proto;
    
    @JsonProperty("service")
    private String service;
    
    @JsonProperty("duration")
    private Double duration;
    
    @JsonProperty("orig_bytes")
    private Long origBytes;
    
    @JsonProperty("resp_bytes")
    private Long respBytes;
    
    @JsonProperty("conn_state")
    private String connState;
    
    @JsonProperty("local_orig")
    private Boolean localOrig;
    
    @JsonProperty("local_resp")
    private Boolean localResp;
    
    @JsonProperty("missed_bytes")
    private Integer missedBytes;
    
    @JsonProperty("history")
    private String history;
    
    @JsonProperty("orig_pkts")
    private Long origPkts;
    
    @JsonProperty("orig_ip_bytes")
    private Long origIpBytes;
    
    @JsonProperty("resp_pkts")
    private Long respPkts;
    
    @JsonProperty("resp_ip_bytes")
    private Long respIpBytes;
    
    @JsonProperty("tunnel_parents")
    private String[] tunnelParents;
    
    // Additional field to handle Zeek log format
    @JsonProperty("zeek-conn")
    private String zeekConn;
    
    // Getters and setters
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
    
    public String getIdOrigH() {
        return idOrigH;
    }
    
    public void setIdOrigH(String idOrigH) {
        this.idOrigH = idOrigH;
    }
    
    public Integer getIdOrigP() {
        return idOrigP;
    }
    
    public void setIdOrigP(Integer idOrigP) {
        this.idOrigP = idOrigP;
    }
    
    public String getIdRespH() {
        return idRespH;
    }
    
    public void setIdRespH(String idRespH) {
        this.idRespH = idRespH;
    }
    
    public Integer getIdRespP() {
        return idRespP;
    }
    
    public void setIdRespP(Integer idRespP) {
        this.idRespP = idRespP;
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
    
    public Double getDuration() {
        return duration;
    }
    
    public void setDuration(Double duration) {
        this.duration = duration;
    }
    
    public Long getOrigBytes() {
        return origBytes;
    }
    
    public void setOrigBytes(Long origBytes) {
        this.origBytes = origBytes;
    }
    
    public Long getRespBytes() {
        return respBytes;
    }
    
    public void setRespBytes(Long respBytes) {
        this.respBytes = respBytes;
    }
    
    public String getConnState() {
        return connState;
    }
    
    public void setConnState(String connState) {
        this.connState = connState;
    }
    
    public Boolean getLocalOrig() {
        return localOrig;
    }
    
    public void setLocalOrig(Boolean localOrig) {
        this.localOrig = localOrig;
    }
    
    public Boolean getLocalResp() {
        return localResp;
    }
    
    public void setLocalResp(Boolean localResp) {
        this.localResp = localResp;
    }
    
    public Integer getMissedBytes() {
        return missedBytes;
    }
    
    public void setMissedBytes(Integer missedBytes) {
        this.missedBytes = missedBytes;
    }
    
    public String getHistory() {
        return history;
    }
    
    public void setHistory(String history) {
        this.history = history;
    }
    
    public Long getOrigPkts() {
        return origPkts;
    }
    
    public void setOrigPkts(Long origPkts) {
        this.origPkts = origPkts;
    }
    
    public Long getOrigIpBytes() {
        return origIpBytes;
    }
    
    public void setOrigIpBytes(Long origIpBytes) {
        this.origIpBytes = origIpBytes;
    }
    
    public Long getRespPkts() {
        return respPkts;
    }
    
    public void setRespPkts(Long respPkts) {
        this.respPkts = respPkts;
    }
    
    public Long getRespIpBytes() {
        return respIpBytes;
    }
    
    public void setRespIpBytes(Long respIpBytes) {
        this.respIpBytes = respIpBytes;
    }
    
    public String[] getTunnelParents() {
        return tunnelParents;
    }
    
    public void setTunnelParents(String[] tunnelParents) {
        this.tunnelParents = tunnelParents;
    }
    
    public String getZeekConn() {
        return zeekConn;
    }
    
    public void setZeekConn(String zeekConn) {
        this.zeekConn = zeekConn;
    }
} 
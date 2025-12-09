package com.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Model class for Zeek DNS log events.
 * Represents the structure of DNS log entries from Zeek.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ZeekDNSLog {
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
    
    // DNS specific fields
    @JsonProperty("proto")
    private String proto;
    
    @JsonProperty("trans_id")
    private Integer transId;
    
    private double rtt;
    
    @JsonProperty("query")
    private String query;
    
    @JsonProperty("qclass")
    private Integer qclass;
    
    @JsonProperty("qclass_name")
    private String qclassName;
    
    @JsonProperty("qtype")
    private Integer qtype;
    
    @JsonProperty("qtype_name")
    private String qtypeName;
    
    @JsonProperty("rcode")
    private Integer rcode;
    
    @JsonProperty("rcode_name")
    private String rcodeName;
    
    @JsonProperty("AA")
    private Boolean aa;
    
    @JsonProperty("TC")
    private Boolean tc;
    
    @JsonProperty("RD")
    private Boolean rd;
    
    @JsonProperty("RA")
    private Boolean ra;
    
    @JsonProperty("Z")
    private Integer z;
    
    @JsonProperty("answers")
    private String[] answers;
    
    @JsonProperty("TTLs")
    private Double[] ttls;
    
    @JsonProperty("rejected")
    private Boolean rejected;
    
    // Additional field to handle Zeek log format
    @JsonProperty("zeek-dns")
    private String zeekDns;
    
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
    
    public Integer getTransId() {
        return transId;
    }
    
    public void setTransId(Integer transId) {
        this.transId = transId;
    }
    
    public double getRtt() { return rtt; }
    public void setRtt(double rtt) { this.rtt = rtt; }
    
    public String getQuery() {
        return query;
    }
    
    public void setQuery(String query) {
        this.query = query;
    }
    
    public Integer getQclass() {
        return qclass;
    }
    
    public void setQclass(Integer qclass) {
        this.qclass = qclass;
    }
    
    public String getQclassName() {
        return qclassName;
    }
    
    public void setQclassName(String qclassName) {
        this.qclassName = qclassName;
    }
    
    public Integer getQtype() {
        return qtype;
    }
    
    public void setQtype(Integer qtype) {
        this.qtype = qtype;
    }
    
    public String getQtypeName() {
        return qtypeName;
    }
    
    public void setQtypeName(String qtypeName) {
        this.qtypeName = qtypeName;
    }
    
    public Integer getRcode() {
        return rcode;
    }
    
    public void setRcode(Integer rcode) {
        this.rcode = rcode;
    }
    
    public String getRcodeName() {
        return rcodeName;
    }
    
    public void setRcodeName(String rcodeName) {
        this.rcodeName = rcodeName;
    }
    
    public Boolean getAa() {
        return aa;
    }
    
    public void setAa(Boolean aa) {
        this.aa = aa;
    }
    
    public Boolean getTc() {
        return tc;
    }
    
    public void setTc(Boolean tc) {
        this.tc = tc;
    }
    
    public Boolean getRd() {
        return rd;
    }
    
    public void setRd(Boolean rd) {
        this.rd = rd;
    }
    
    public Boolean getRa() {
        return ra;
    }
    
    public void setRa(Boolean ra) {
        this.ra = ra;
    }
    
    public Integer getZ() {
        return z;
    }
    
    public void setZ(Integer z) {
        this.z = z;
    }
    
    public String[] getAnswers() {
        return answers;
    }
    
    public void setAnswers(String[] answers) {
        this.answers = answers;
    }
    
    public Double[] getTtls() {
        return ttls;
    }
    
    public void setTtls(Double[] ttls) {
        this.ttls = ttls;
    }
    
    public Boolean getRejected() {
        return rejected;
    }
    
    public void setRejected(Boolean rejected) {
        this.rejected = rejected;
    }
    
    public String getZeekDns() {
        return zeekDns;
    }
    
    public void setZeekDns(String zeekDns) {
        this.zeekDns = zeekDns;
    }
} 
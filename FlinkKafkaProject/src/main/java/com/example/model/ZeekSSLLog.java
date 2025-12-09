package com.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Model class for Zeek SSL log events.
 * Represents the structure of SSL log entries from Zeek.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ZeekSSLLog {
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
    
    // SSL specific fields
    @JsonProperty("version")
    private String version;
    
    @JsonProperty("cipher")
    private String cipher;
    
    @JsonProperty("curve")
    private String curve;
    
    @JsonProperty("server_name")
    private String serverName;
    
    @JsonProperty("resumed")
    private Boolean resumed;
    
    @JsonProperty("established")
    private Boolean established;
    
    @JsonProperty("cert_chain_fuids")
    private String[] certChainFuids;
    
    @JsonProperty("subject")
    private String subject;
    
    @JsonProperty("issuer")
    private String issuer;
    
    @JsonProperty("session_id")
    private String sessionId;
    
    @JsonProperty("last_alert")
    private String lastAlert;
    
    @JsonProperty("client_cert_chain_fuids")
    private String[] clientCertChainFuids;
    
    @JsonProperty("client_subject")
    private String clientSubject;
    
    @JsonProperty("client_issuer")
    private String clientIssuer;
    
    @JsonProperty("validation_status")
    private String validationStatus;
    
    // Additional field to handle Zeek log format
    @JsonProperty("zeek-ssl")
    private String zeekSsl;
    
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
    
    public String getVersion() {
        return version;
    }
    
    public void setVersion(String version) {
        this.version = version;
    }
    
    public String getCipher() {
        return cipher;
    }
    
    public void setCipher(String cipher) {
        this.cipher = cipher;
    }
    
    public String getCurve() {
        return curve;
    }
    
    public void setCurve(String curve) {
        this.curve = curve;
    }
    
    public String getServerName() {
        return serverName;
    }
    
    public void setServerName(String serverName) {
        this.serverName = serverName;
    }
    
    public Boolean getResumed() {
        return resumed;
    }
    
    public void setResumed(Boolean resumed) {
        this.resumed = resumed;
    }
    
    public Boolean getEstablished() {
        return established;
    }
    
    public void setEstablished(Boolean established) {
        this.established = established;
    }
    
    public String[] getCertChainFuids() {
        return certChainFuids;
    }
    
    public void setCertChainFuids(String[] certChainFuids) {
        this.certChainFuids = certChainFuids;
    }
    
    public String getSubject() {
        return subject;
    }
    
    public void setSubject(String subject) {
        this.subject = subject;
    }
    
    public String getIssuer() {
        return issuer;
    }
    
    public void setIssuer(String issuer) {
        this.issuer = issuer;
    }
    
    public String getSessionId() {
        return sessionId;
    }
    
    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
    
    public String getLastAlert() {
        return lastAlert;
    }
    
    public void setLastAlert(String lastAlert) {
        this.lastAlert = lastAlert;
    }
    
    public String[] getClientCertChainFuids() {
        return clientCertChainFuids;
    }
    
    public void setClientCertChainFuids(String[] clientCertChainFuids) {
        this.clientCertChainFuids = clientCertChainFuids;
    }
    
    public String getClientSubject() {
        return clientSubject;
    }
    
    public void setClientSubject(String clientSubject) {
        this.clientSubject = clientSubject;
    }
    
    public String getClientIssuer() {
        return clientIssuer;
    }
    
    public void setClientIssuer(String clientIssuer) {
        this.clientIssuer = clientIssuer;
    }
    
    public String getValidationStatus() {
        return validationStatus;
    }
    
    public void setValidationStatus(String validationStatus) {
        this.validationStatus = validationStatus;
    }
    
    public String getZeekSsl() {
        return zeekSsl;
    }
    
    public void setZeekSsl(String zeekSsl) {
        this.zeekSsl = zeekSsl;
    }
} 
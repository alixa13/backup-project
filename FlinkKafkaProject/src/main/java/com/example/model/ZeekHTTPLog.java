package com.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Model class for Zeek HTTP log events.
 * Represents the structure of HTTP log entries from Zeek.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ZeekHTTPLog {
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
    
    // HTTP specific fields
    @JsonProperty("trans_depth")
    private Integer transDepth;
    
    @JsonProperty("method")
    private String method;
    
    @JsonProperty("host")
    private String host;
    
    @JsonProperty("uri")
    private String uri;
    
    @JsonProperty("referrer")
    private String referrer;
    
    @JsonProperty("version")
    private String version;
    
    @JsonProperty("user_agent")
    private String userAgent;
    
    @JsonProperty("request_body_len")
    private Long requestBodyLen;
    
    @JsonProperty("response_body_len")
    private Long responseBodyLen;
    
    @JsonProperty("status_code")
    private Integer statusCode;
    
    @JsonProperty("status_msg")
    private String statusMsg;
    
    @JsonProperty("info_code")
    private Integer infoCode;
    
    @JsonProperty("info_msg")
    private String infoMsg;
    
    @JsonProperty("filename")
    private String filename;
    
    @JsonProperty("tags")
    private String[] tags;
    
    @JsonProperty("username")
    private String username;
    
    @JsonProperty("password")
    private String password;
    
    @JsonProperty("proxied")
    private String[] proxied;
    
    @JsonProperty("orig_fuids")
    private String[] origFuids;
    
    @JsonProperty("orig_filenames")
    private String[] origFilenames;
    
    @JsonProperty("orig_mime_types")
    private String[] origMimeTypes;
    
    @JsonProperty("resp_fuids")
    private String[] respFuids;
    
    @JsonProperty("resp_filenames")
    private String[] respFilenames;
    
    @JsonProperty("resp_mime_types")
    private String[] respMimeTypes;
    
    // Additional field to handle Zeek log format
    @JsonProperty("zeek-http")
    private String zeekHttp;
    
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
    
    public Integer getTransDepth() {
        return transDepth;
    }
    
    public void setTransDepth(Integer transDepth) {
        this.transDepth = transDepth;
    }
    
    public String getMethod() {
        return method;
    }
    
    public void setMethod(String method) {
        this.method = method;
    }
    
    public String getHost() {
        return host;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public String getUri() {
        return uri;
    }
    
    public void setUri(String uri) {
        this.uri = uri;
    }
    
    public String getReferrer() {
        return referrer;
    }
    
    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }
    
    public String getVersion() {
        return version;
    }
    
    public void setVersion(String version) {
        this.version = version;
    }
    
    public String getUserAgent() {
        return userAgent;
    }
    
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }
    
    public Long getRequestBodyLen() {
        return requestBodyLen;
    }
    
    public void setRequestBodyLen(Long requestBodyLen) {
        this.requestBodyLen = requestBodyLen;
    }
    
    public Long getResponseBodyLen() {
        return responseBodyLen;
    }
    
    public void setResponseBodyLen(Long responseBodyLen) {
        this.responseBodyLen = responseBodyLen;
    }
    
    public Integer getStatusCode() {
        return statusCode;
    }
    
    public void setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
    }
    
    public String getStatusMsg() {
        return statusMsg;
    }
    
    public void setStatusMsg(String statusMsg) {
        this.statusMsg = statusMsg;
    }
    
    public Integer getInfoCode() {
        return infoCode;
    }
    
    public void setInfoCode(Integer infoCode) {
        this.infoCode = infoCode;
    }
    
    public String getInfoMsg() {
        return infoMsg;
    }
    
    public void setInfoMsg(String infoMsg) {
        this.infoMsg = infoMsg;
    }
    
    public String getFilename() {
        return filename;
    }
    
    public void setFilename(String filename) {
        this.filename = filename;
    }
    
    public String[] getTags() {
        return tags;
    }
    
    public void setTags(String[] tags) {
        this.tags = tags;
    }
    
    public String getUsername() {
        return username;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public String[] getProxied() {
        return proxied;
    }
    
    public void setProxied(String[] proxied) {
        this.proxied = proxied;
    }
    
    public String[] getOrigFuids() {
        return origFuids;
    }
    
    public void setOrigFuids(String[] origFuids) {
        this.origFuids = origFuids;
    }
    
    public String[] getOrigFilenames() {
        return origFilenames;
    }
    
    public void setOrigFilenames(String[] origFilenames) {
        this.origFilenames = origFilenames;
    }
    
    public String[] getOrigMimeTypes() {
        return origMimeTypes;
    }
    
    public void setOrigMimeTypes(String[] origMimeTypes) {
        this.origMimeTypes = origMimeTypes;
    }
    
    public String[] getRespFuids() {
        return respFuids;
    }
    
    public void setRespFuids(String[] respFuids) {
        this.respFuids = respFuids;
    }
    
    public String[] getRespFilenames() {
        return respFilenames;
    }
    
    public void setRespFilenames(String[] respFilenames) {
        this.respFilenames = respFilenames;
    }
    
    public String[] getRespMimeTypes() {
        return respMimeTypes;
    }
    
    public void setRespMimeTypes(String[] respMimeTypes) {
        this.respMimeTypes = respMimeTypes;
    }
    
    public String getZeekHttp() {
        return zeekHttp;
    }
    
    public void setZeekHttp(String zeekHttp) {
        this.zeekHttp = zeekHttp;
    }
} 
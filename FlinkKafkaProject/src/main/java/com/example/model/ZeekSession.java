package com.example.model;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a complete session for one connection (UID).
 * Conn is the anchor; dns, http, ssl are optional protocol logs.
 */
public class ZeekSession {
    private String uid;
    private JsonNode conn;
    private final List<JsonNode> dns = new ArrayList<>();
    private final List<JsonNode> http = new ArrayList<>();
    private final List<JsonNode> ssl = new ArrayList<>();

    public ZeekSession() {}

    public ZeekSession(String uid) {
        this.uid = uid;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public JsonNode getConn() {
        return conn;
    }

    public void setConn(JsonNode conn) {
        this.conn = conn;
    }

    public List<JsonNode> getDns() {
        return dns;
    }

    public List<JsonNode> getHttp() {
        return http;
    }

    public List<JsonNode> getSsl() {
        return ssl;
    }

    public void addDns(JsonNode log) {
        if (log != null) dns.add(log);
    }

    public void addHttp(JsonNode log) {
        if (log != null) http.add(log);
    }

    public void addSsl(JsonNode log) {
        if (log != null) ssl.add(log);
    }

    /** True if we have the conn anchor (required for session). */
    public boolean hasConn() {
        return conn != null;
    }
}

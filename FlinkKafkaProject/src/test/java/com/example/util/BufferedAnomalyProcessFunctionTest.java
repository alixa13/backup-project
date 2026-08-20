package com.example.util;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the real operator in Flink's test harness against a stub ML API.
 *
 * <p>The static block below has to run before anything touches APIConfig, whose static
 * initializer reads the properties file once.
 */
class BufferedAnomalyProcessFunctionTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpServer SERVER;
    private static final AtomicInteger predictCalls = new AtomicInteger();
    private static final AtomicInteger learnCalls = new AtomicInteger();
    private static final AtomicInteger learnRecords = new AtomicInteger();

    static {
        try {
            SERVER = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            SERVER.createContext("/", BufferedAnomalyProcessFunctionTest::handle);
            SERVER.setExecutor(null);
            SERVER.start();
            String base = "http://127.0.0.1:" + SERVER.getAddress().getPort();

            Path cfg = Files.createTempFile("api-config", ".properties");
            Files.write(cfg, ("unsupervised.api.base.url=" + base + "\n"
                    + "supervised.api.base.url=" + base + "\n"
                    + "api.retry.count=1\napi.retry.delay=1\napi.retry.backoff.max.ms=1\n"
                    + "api.connection.timeout=2000\napi.read.timeout=2000\n")
                    .getBytes(StandardCharsets.UTF_8));
            System.setProperty("api.config.file", cfg.toString());
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static void handle(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        byte[] body;
        try (InputStream is = ex.getRequestBody()) {
            body = is.readAllBytes();
        }
        String resp;
        if (path.startsWith("/learning/status/")) {
            // "dns" is the learning-mode fixture; everything else is in detection mode.
            resp = "{\"learning_enabled\": " + path.endsWith("/dns") + "}";
        } else if (path.startsWith("/threshold/")) {
            resp = "{\"has_threshold\": true, \"threshold\": 0.5}";
        } else if (path.startsWith("/buffer/update/")) {
            resp = "{\"status\": \"ok\"}";
        } else if (path.startsWith("/health")) {
            resp = "{\"status\": \"ok\"}";
        } else if (path.startsWith("/predict/dns")) {
            learnCalls.incrementAndGet();
            learnRecords.addAndGet(countData(body));
            resp = "{\"status\":\"success\",\"learning_enabled\":true}";
        } else if (path.startsWith("/predict/")) {
            predictCalls.incrementAndGet();
            int n = countData(body);
            StringBuilder sb = new StringBuilder("{\"status\":\"success\",\"results\":[");
            for (int i = 0; i < n; i++) {
                // exactly one anomaly per batch, at index 0
                sb.append(i == 0 ? "{\"mae\":0.9}" : "{\"mae\":0.1}");
                if (i < n - 1) sb.append(',');
            }
            resp = sb.append("]}").toString();
        } else {
            resp = "{}";
        }
        byte[] out = resp.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(200, out.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(out);
        }
    }

    private static int countData(byte[] body) {
        try {
            JsonNode n = MAPPER.readTree(body).get("data");
            return n == null ? 0 : n.size();
        } catch (Exception e) {
            return 0;
        }
    }

    @AfterAll
    static void stopServer() {
        SERVER.stop(0);
    }

    @BeforeEach
    void reset() {
        predictCalls.set(0);
        learnCalls.set(0);
        learnRecords.set(0);
    }

    private static BufferedAnomalyProcessFunction fn(String logType) {
        return new BufferedAnomalyProcessFunction(
                logType, "zeek-" + logType, 33,
                /* predictionBatchSize */ 50, /* predictionBatchTimeoutMs */ 200L,
                /* learningBatchSize */ 10_000, /* learningBatchTimeoutMs */ 5_000L,
                /* defaultThreshold */ 0.5, /* thresholdRefreshMs */ 300_000L,
                /* windowSize */ 10, /* maxActiveSessions */ 5000,
                Arrays.asList(21, 22, 53, 80, 443, 8080, 3306, 445));
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, JsonNode, String> harness(
            BufferedAnomalyProcessFunction function, String logType) throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, JsonNode, String> h =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(function),
                        new BufferedAnomalyProcessFunction.SourceIpKeySelector("zeek-" + logType),
                        BasicTypeInfo.STRING_TYPE_INFO);
        h.open();
        return h;
    }

    private static JsonNode record(String logType, String srcIp, int i) throws Exception {
        String json = "{\"zeek-" + logType + "\":{"
                + "\"ts\":" + (1700000000 + i) + ".5,"
                + "\"id.orig_h\":\"" + srcIp + "\",\"id.resp_h\":\"10.1.1.1\","
                + "\"id.orig_p\":" + (40000 + i) + ",\"id.resp_p\":443,"
                + "\"duration\":0.25,\"orig_bytes\":120,\"resp_bytes\":340,"
                + "\"orig_pkts\":3,\"resp_pkts\":4,\"conn_state\":\"SF\"}}";
        return MAPPER.readTree(json);
    }

    @Test
    void flushesWhenBatchSizeIsReached() throws Exception {
        BufferedAnomalyProcessFunction f = fn("conn");
        try (KeyedOneInputStreamOperatorTestHarness<String, JsonNode, String> h = harness(f, "conn")) {
            for (int i = 0; i < 50; i++) {
                h.processElement(new StreamRecord<>(record("conn", "192.168.0." + (i % 8), i)));
            }
            assertEquals(1, predictCalls.get(), "one batch of 50 should be one API call");
            assertEquals(0, f.pendingPredictionCount(), "buffer drained after a size flush");
            assertEquals(1, h.extractOutputValues().size(), "one record over threshold emitted");
        }
    }

    /** Regression for the buffer that was only ever checked when a new record arrived. */
    @Test
    void timerFlushesPartialBufferWhenTrafficStops() throws Exception {
        BufferedAnomalyProcessFunction f = fn("conn");
        try (KeyedOneInputStreamOperatorTestHarness<String, JsonNode, String> h = harness(f, "conn")) {
            h.setProcessingTime(1_000L);
            for (int i = 0; i < 10; i++) {
                h.processElement(new StreamRecord<>(record("conn", "192.168.0." + i, i)));
            }
            assertEquals(0, predictCalls.get(), "under the batch size, nothing sent yet");
            assertEquals(10, f.pendingPredictionCount(), "10 records waiting");

            // traffic stops; only the clock moves
            h.setProcessingTime(1_000L + 250L);

            assertEquals(1, predictCalls.get(), "timer must flush the partial buffer");
            assertEquals(0, f.pendingPredictionCount(), "buffer drained by the timer");
            assertEquals(1, h.extractOutputValues().size(), "anomaly emitted from the timer");
        }
    }

    /** Regression for the static buffers shared by every subtask in the JVM. */
    @Test
    void buffersAreNotSharedBetweenInstances() throws Exception {
        BufferedAnomalyProcessFunction a = fn("conn");
        BufferedAnomalyProcessFunction b = fn("conn");
        try (KeyedOneInputStreamOperatorTestHarness<String, JsonNode, String> ha = harness(a, "conn");
             KeyedOneInputStreamOperatorTestHarness<String, JsonNode, String> hb = harness(b, "conn")) {
            for (int i = 0; i < 5; i++) {
                ha.processElement(new StreamRecord<>(record("conn", "192.168.0." + i, i)));
            }
            assertEquals(5, a.pendingPredictionCount(), "records landed in instance a");
            assertEquals(0, b.pendingPredictionCount(), "instance b must not see them");
            assertTrue(hb.extractOutputValues().isEmpty(), "b emitted nothing");
        }
    }

    @Test
    void closeFlushesLearningBuffer() throws Exception {
        BufferedAnomalyProcessFunction f = fn("dns");   // stub reports learning mode for dns
        KeyedOneInputStreamOperatorTestHarness<String, JsonNode, String> h = harness(f, "dns");
        for (int i = 0; i < 7; i++) {
            h.processElement(new StreamRecord<>(record("dns", "192.168.1." + i, i)));
        }
        assertEquals(7, f.pendingLearningCount(), "learning records buffered, not sent");
        assertEquals(0, learnCalls.get(), "nothing sent before shutdown");

        h.close();

        assertEquals(1, learnCalls.get(), "close() must drain the learning buffer");
        assertEquals(7, learnRecords.get(), "all 7 records reached the learning API");
        assertEquals(0, f.pendingLearningCount());
    }
}

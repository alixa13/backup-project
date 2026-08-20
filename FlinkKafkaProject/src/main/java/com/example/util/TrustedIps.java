package com.example.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Loads trusted IPs from a text file (one IP per line).
 * Used to skip anomaly checks for these IPs in both unsupervised (LSTM) and supervised flows.
 * File path: system property "trusted.ips.file", env TRUSTED_IPS_FILE, or /opt/flink/config/trusted_ips.txt
 */
public final class TrustedIps {
    private static final Logger LOG = Logger.getLogger(TrustedIps.class.getName());
    private static final String DEFAULT_PATH = "/opt/flink/config/trusted_ips.txt";
    private static volatile Set<String> trusted = loadTrustedIps();

    private TrustedIps() {}

    private static String getTrustedIpsPath() {
        String path = System.getProperty("trusted.ips.file");
        if (path != null && !path.isEmpty()) return path;
        path = System.getenv("TRUSTED_IPS_FILE");
        if (path != null && !path.isEmpty()) return path;
        return DEFAULT_PATH;
    }

    private static Set<String> loadTrustedIps() {
        String path = getTrustedIpsPath();
        File file = new File(path);
        if (!file.isFile()) {
            LOG.info("Trusted IPs file not found or not a file: " + path + " (trusted set empty)");
            return Collections.emptySet();
        }
        Set<String> set = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String ip = line.trim();
                if (!ip.isEmpty() && !ip.startsWith("#")) {
                    set.add(ip);
                }
            }
            LOG.info("Loaded " + set.size() + " trusted IPs from " + path);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Failed to load trusted IPs from " + path, e);
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(set);
    }

    /**
     * Returns true if the given IP is in the trusted list (and should be ignored for anomaly checks).
     */
    public static boolean isTrusted(String ip) {
        if (ip == null || ip.isEmpty()) return false;
        return trusted.contains(ip.trim());
    }

    /**
     * Reload trusted IPs from file (e.g. after updating the file on disk).
     */
    public static void reload() {
        trusted = loadTrustedIps();
    }
}

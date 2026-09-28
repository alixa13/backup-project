package io.netsecml.platform.bootstrap.online;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.adapter.kafka.mapper.S7commEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekS7commParser;
import io.netsecml.platform.application.usecase.S7commBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.ConnEvent;
import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.ModbusEvent;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.FeatureBuildResult;
import io.netsecml.platform.domain.feature.S7commCategories;
import io.netsecml.platform.domain.feature.S7commConnectionState;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

// Exports S7comm feature vectors for training
// (docs/superpowers/specs/2026-09-28-s7comm-detector-v2-design.md section 5):
// every line of a Zeek s7comm.log goes through the online job's own parser,
// mapper and S7commBuildFeaturesUseCase, with one connection state per uid
// exactly as S7commFeatureProcessFunction keeps it, and becomes one CSV row.
// Training reads these rows and never computes a feature itself, so a model
// and the live scorer cannot disagree about what a feature is.
public final class S7commFeatureExport {

    // The CSV columns, in order: identity and context, then f0..f15 (the
    // s7comm-feature-v1 values), then the vector's quality flags.
    public static final List<String> COLUMNS = columns();

    // How many rows one capture gave, and how many records it rejected.
    public record Summary(int rows, int rejected) {
    }

    // The sensor id only labels event ids.
    private static final SensorId SENSOR = new SensorId("s7comm-feature-export");
    private static final ObjectMapper JSON = new ObjectMapper();

    private S7commFeatureExport() {
    }

    private static List<String> columns() {
        List<String> c = new ArrayList<>(List.of("capture", "uid", "ts", "event_id", "client_ip", "server_ip",
            "is_request", "function_code", "fresh", "rosctr", "operation"));
        for (int i = 0; i < 16; i++) {
            c.add("f" + i);
        }
        c.add("quality_flags");
        return List.copyOf(c);
    }

    // One capture's records, in order, with one connection state per uid. A
    // rejected record becomes one JSON line in `rejects`, with its reason.
    public static Summary export(String capture, List<String> lines, Appendable rows, Appendable rejects)
            throws IOException {
        if (capture.isEmpty() || capture.contains(",")) {
            throw new IllegalArgumentException("a capture name must be non-empty and contain no comma: " + capture);
        }
        S7commBuildFeaturesUseCase useCase = new S7commBuildFeaturesUseCase(Clock.systemUTC());
        Map<String, S7commConnectionState> states = new HashMap<>();
        int written = 0;
        int rejected = 0;
        for (String line : lines) {
            if (line.isBlank()) {
                continue;
            }
            MappingResult<NetworkEvent> mapped = parseAndMap(line);
            if (!mapped.isValid()) {
                rejected++;
                ObjectNode reject = JSON.createObjectNode();
                reject.put("capture", capture);
                reject.put("reason", String.valueOf(mapped.reason()));
                reject.put("detail", mapped.detail());
                reject.put("line", line);
                rejects.append(JSON.writeValueAsString(reject)).append('\n');
                continue;
            }
            S7commEvent event = narrow(mapped.value());
            // As S7commFeatureProcessFunction: absent state is a fresh connection.
            S7commConnectionState state = states.get(event.connectionUid());
            boolean fresh = state == null;
            FeatureBuildResult<S7commConnectionState> built =
                useCase.build(event, fresh ? S7commConnectionState.empty() : state);
            states.put(event.connectionUid(), built.newState());
            rows.append(row(capture, event, fresh, built.vector().values(), built.vector().qualityFlags()))
                .append('\n');
            written++;
        }
        return new Summary(written, rejected);
    }

    // The online job's two stages: parse, then map.
    private static MappingResult<NetworkEvent> parseAndMap(String line) {
        MappingResult<ZeekS7commRecord> parsed = new JsonZeekS7commParser().parse(line.getBytes(StandardCharsets.UTF_8));
        return parsed.isValid()
            ? new S7commEventMapper().map(parsed.value(), SENSOR)
            : MappingResult.invalid(parsed.reason(), parsed.detail());
    }

    // S7commEventMapper only ever produces an S7commEvent; any other is a wiring error.
    private static S7commEvent narrow(NetworkEvent event) {
        return switch (event) {
            case S7commEvent s7 -> s7;
            case ConnEvent ignored -> throw new IllegalStateException("S7commEventMapper produced a ConnEvent");
            case DnsEvent ignored -> throw new IllegalStateException("S7commEventMapper produced a DnsEvent");
            case ModbusEvent ignored -> throw new IllegalStateException("S7commEventMapper produced a ModbusEvent");
        };
    }

    // One CSV row. Floats print with Float.toString, which reads back exactly;
    // the categories print as S7commCategories decodes them.
    private static String row(String capture, S7commEvent event, boolean fresh, float[] v, int flags) {
        String client = event.isRequest() ? event.sourceIp() : event.destinationIp();
        String server = event.isRequest() ? event.destinationIp() : event.sourceIp();
        StringBuilder b = new StringBuilder();
        b.append(capture).append(',').append(event.connectionUid()).append(',')
            .append(String.format(Locale.ROOT, "%.6f", event.tsSeconds())).append(',')
            .append(event.eventId().value()).append(',').append(client).append(',').append(server).append(',')
            .append(event.isRequest() ? 1 : 0).append(',')
            .append(event.functionCode() == null ? "" : event.functionCode().toString()).append(',')
            .append(fresh ? 1 : 0).append(',')
            .append(S7commCategories.decodeRosctr((int) v[14])).append(',')
            .append(S7commCategories.decodeOperation((int) v[15]));
        for (float value : v) {
            b.append(',').append(Float.toString(value));
        }
        return b.append(',').append(flags).toString();
    }

    // usage: S7commFeatureExport --out FILE --rejects FILE CAPTURE=LOG [CAPTURE=LOG ...]
    public static void main(String[] args) {
        System.exit(run(args, System.out));
    }

    // Exit status: 0 when rows were written; 2 for bad arguments or I/O; 3 when
    // the logs held no records at all, so nothing was exported.
    static int run(String[] args, PrintStream out) {
        if (args.length < 5 || !args[0].equals("--out") || !args[2].equals("--rejects")) {
            out.println("usage: S7commFeatureExport --out FILE --rejects FILE CAPTURE=LOG [CAPTURE=LOG ...]");
            return 2;
        }
        int total = 0;
        try (Writer rows = Files.newBufferedWriter(Path.of(args[1]), StandardCharsets.UTF_8);
             Writer rejects = Files.newBufferedWriter(Path.of(args[3]), StandardCharsets.UTF_8)) {
            rows.append(String.join(",", COLUMNS)).append('\n');
            for (int i = 4; i < args.length; i++) {
                int eq = args[i].indexOf('=');
                if (eq <= 0) {
                    out.println("expected CAPTURE=LOG, got " + args[i]);
                    return 2;
                }
                String capture = args[i].substring(0, eq);
                List<String> lines = Files.readAllLines(Path.of(args[i].substring(eq + 1)), StandardCharsets.UTF_8);
                Summary s = export(capture, lines, rows, rejects);
                out.printf("%s: %d rows, %d rejected%n", capture, s.rows(), s.rejected());
                total += s.rows();
            }
        } catch (IOException | IllegalArgumentException e) {
            out.println("cannot export: " + e.getMessage());
            return 2;
        }
        return total == 0 ? 3 : 0;
    }
}

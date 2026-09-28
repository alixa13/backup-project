package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.kafka.dto.ZeekModbusRecord;
import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.adapter.kafka.mapper.ModbusEventMapper;
import io.netsecml.platform.adapter.kafka.mapper.S7commEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekModbusParser;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekS7commParser;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.ReasonCode;
import io.netsecml.platform.domain.event.SensorId;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

// Checks real sensor output against the production parsers: every JSON line of
// a Zeek modbus_detailed or s7comm log goes through the same parser and mapper
// the online job uses, and each rejection is reported. deploy.sh zeek-check runs
// it on the server -- over ICSNPP's sample traces put through our Zeek image,
// and over the newest records on the live raw topics -- and ZeekRecordCheckTest
// pins it here against the committed real-output fixtures.
//
// It never changes what the parsers accept. A sensor whose records they reject
// is a finding to report: the parsers mirror the frozen models' training input
// (docs/superpowers/specs/2026-09-24-server-deployment-design.md §10).
public final class ZeekRecordCheck {

    public enum Protocol { MODBUS, S7COMM }

    // One rejected record and the reason the parser or mapper gave.
    public record Rejection(Protocol protocol, ReasonCode reason, String detail, String line) {

        // A rejection upstream's own engine makes too: a Modbus function name its
        // FUNCTION_NAME_TO_CODE table cannot resolve -- Zeek's <NAME>_EXCEPTION
        // PDUs, and ENCAP_INTERFACE_TRANSPORT, which upstream spells
        // ENCAPSULATED_INTERFACE_TRANSPORT (CLAUDE.md, Modbus limits, F4). Any
        // other rejection means the sensor's output and the parsers disagree.
        public boolean knownUpstreamParity() {
            return protocol == Protocol.MODBUS
                && reason == ReasonCode.MISSING_REQUIRED_FIELD
                && detail.startsWith("func did not resolve to a known function code");
        }
    }

    // The outcome for one protocol's records.
    public record Report(Protocol protocol, int accepted, List<Rejection> rejections) {

        public Report {
            rejections = List.copyOf(rejections);
        }

        public int records() {
            return accepted + rejections.size();
        }

        public boolean passes() {
            return rejections.stream().allMatch(Rejection::knownUpstreamParity);
        }
    }

    // The sensor id only labels event ids, which this check never looks at.
    private static final SensorId SENSOR = new SensorId("zeek-record-check");

    private ZeekRecordCheck() {
    }

    // Parse and map every non-blank line as one record of the given protocol.
    public static Report check(Protocol protocol, List<String> lines) {
        int accepted = 0;
        List<Rejection> rejections = new ArrayList<>();
        for (String line : lines) {
            if (line.isBlank()) {
                continue;
            }
            MappingResult<NetworkEvent> result = parseAndMap(protocol, line.getBytes(StandardCharsets.UTF_8));
            if (result.isValid()) {
                accepted++;
            } else {
                rejections.add(new Rejection(protocol, result.reason(), result.detail(), line));
            }
        }
        return new Report(protocol, accepted, rejections);
    }

    // The online job's own two stages for the protocol: parse, then map. A parse
    // failure is carried through as it is, with the parser's reason.
    private static MappingResult<NetworkEvent> parseAndMap(Protocol protocol, byte[] json) {
        return switch (protocol) {
            case MODBUS -> {
                MappingResult<ZeekModbusRecord> parsed = new JsonZeekModbusParser().parse(json);
                yield parsed.isValid()
                    ? new ModbusEventMapper().map(parsed.value(), SENSOR)
                    : MappingResult.invalid(parsed.reason(), parsed.detail());
            }
            case S7COMM -> {
                MappingResult<ZeekS7commRecord> parsed = new JsonZeekS7commParser().parse(json);
                yield parsed.isValid()
                    ? new S7commEventMapper().map(parsed.value(), SENSOR)
                    : MappingResult.invalid(parsed.reason(), parsed.detail());
            }
        };
    }

    // usage: ZeekRecordCheck (--modbus|--s7comm) FILE [(--modbus|--s7comm) FILE ...]
    public static void main(String[] args) {
        System.exit(run(args, System.out));
    }

    // Exit status: 0 when every rejection is a known upstream-parity one; 1 when
    // one is not; 2 for bad arguments or an unreadable file; 3 when there were no
    // records at all (a live topic with no traffic yet), so nothing was proven.
    static int run(String[] args, PrintStream out) {
        if (args.length == 0 || args.length % 2 != 0) {
            out.println("usage: ZeekRecordCheck (--modbus|--s7comm) FILE [(--modbus|--s7comm) FILE ...]");
            return 2;
        }
        List<Report> reports = new ArrayList<>();
        for (int i = 0; i < args.length; i += 2) {
            // The option names the protocol whose parser reads the next file.
            Protocol protocol = switch (args[i]) {
                case "--modbus" -> Protocol.MODBUS;
                case "--s7comm" -> Protocol.S7COMM;
                default -> null;
            };
            if (protocol == null) {
                out.println("unknown option " + args[i] + "; expected --modbus or --s7comm");
                return 2;
            }
            List<String> lines;
            try {
                lines = Files.readAllLines(Path.of(args[i + 1]), StandardCharsets.UTF_8);
            } catch (IOException e) {
                out.println("cannot read " + args[i + 1] + ": " + e.getMessage());
                return 2;
            }
            Report report = check(protocol, lines);
            reports.add(report);
            print(report, args[i + 1], out);
        }

        // Nothing checked is its own outcome, never a pass.
        if (reports.stream().mapToInt(Report::records).sum() == 0) {
            out.println("RESULT: NO RECORDS -- nothing was checked");
            return 3;
        }
        boolean pass = reports.stream().allMatch(Report::passes);
        out.println(pass ? "RESULT: PASS" : "RESULT: FAIL -- the sensor's records and the parsers disagree");
        return pass ? 0 : 1;
    }

    // One summary line per protocol, then one line per rejection.
    private static void print(Report report, String file, PrintStream out) {
        long known = report.rejections().stream().filter(Rejection::knownUpstreamParity).count();
        out.printf("%s (%s): %d records, %d accepted, %d rejected (%d known upstream parity, %d unexpected)%n",
            report.protocol().name().toLowerCase(Locale.ROOT), file, report.records(), report.accepted(),
            report.rejections().size(), known, report.rejections().size() - known);
        for (Rejection r : report.rejections()) {
            out.printf("  %s %s %s :: %s%n", r.knownUpstreamParity() ? "known     " : "UNEXPECTED",
                r.reason(), r.detail(), abbreviate(r.line()));
        }
    }

    // Long records are cut so one bad line cannot flood the terminal.
    private static String abbreviate(String line) {
        return line.length() <= 240 ? line : line.substring(0, 240) + "...";
    }
}

package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.bootstrap.online.ZeekRecordCheck.Protocol;
import io.netsecml.platform.bootstrap.online.ZeekRecordCheck.Rejection;
import io.netsecml.platform.bootstrap.online.ZeekRecordCheck.Report;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

// Pins the parsers against REAL Zeek output. tests/fixtures/zeek/ holds what the
// netsec-ml Zeek image (Zeek 7.0.9, icsnpp-modbus v1.0.0, icsnpp-s7comm 7ebeb03)
// wrote for ICSNPP's own sample traces; every other parser test builds its
// records by hand, these were written by the sensor itself.
class ZeekRecordCheckTest {

    private static final Path FIXTURES = Path.of("..", "..", "tests", "fixtures", "zeek");
    private static final String MODBUS = "icsnpp-modbus-v1.0.0_modbus_detailed.jsonl";
    private static final String S7COMM = "icsnpp-s7comm-7ebeb03_s7comm.jsonl";
    private static final Pattern FUNC = Pattern.compile("\"func\":\"([^\"]+)\"");

    @Test
    void everyRealIcsnppS7commRecordIsAccepted() throws IOException {
        Report report = ZeekRecordCheck.check(Protocol.S7COMM, fixture(S7COMM));
        assertEquals(84, report.accepted());
        assertEquals(List.of(), report.rejections());
        assertTrue(report.passes());
    }

    // 45 of 48 are accepted. The other three are rejections upstream's own engine
    // makes: Zeek names function 43 ENCAP_INTERFACE_TRANSPORT where upstream's
    // table says ENCAPSULATED_INTERFACE_TRANSPORT, and an exception PDU's name
    // resolves to no function at all (CLAUDE.md, Modbus limits, F4).
    @Test
    void realIcsnppModbusRecordsAreAcceptedExceptUpstreamsOwnUnresolvableFunctionNames() throws IOException {
        Report report = ZeekRecordCheck.check(Protocol.MODBUS, fixture(MODBUS));
        assertEquals(45, report.accepted());
        assertEquals(List.of("ENCAP_INTERFACE_TRANSPORT", "ENCAP_INTERFACE_TRANSPORT",
                "READ_HOLDING_REGISTERS_EXCEPTION"),
            report.rejections().stream().map(r -> funcOf(r.line())).toList());
        assertTrue(report.rejections().stream().allMatch(Rejection::knownUpstreamParity));
        assertTrue(report.passes());
    }

    // icsnpp-modbus v2.0.0's shape -- one record per request/response pair, no
    // is_orig, no request_response -- must be rejected loudly, never misread.
    @Test
    void aRecordInIcsnppModbusV2ShapeIsAnUnexpectedRejection() {
        String v2 = "{\"ts\":1595660608.237928,\"uid\":\"CfkkU2CEsCsYzgPNf\",\"id_orig_h\":\"127.0.0.1\","
            + "\"id_orig_p\":47785,\"id_resp_h\":\"127.0.0.1\",\"id_resp_p\":502,\"tid\":1,\"unit\":4,"
            + "\"func\":\"READ_COILS\",\"address\":1,\"quantity\":1,"
            + "\"response_values\":[1,0,0,0,0,0,0,0],\"matched\":true}";
        Report report = ZeekRecordCheck.check(Protocol.MODBUS, List.of(v2));
        assertEquals(0, report.accepted());
        assertEquals(1, report.rejections().size());
        assertFalse(report.rejections().get(0).knownUpstreamParity());
        assertFalse(report.passes());
    }

    @Test
    void blankLinesAreNotRecords() {
        assertEquals(0, ZeekRecordCheck.check(Protocol.S7COMM, List.of("", "   ")).records());
    }

    @Test
    void runPassesOnTheFixturesAndSaysSo() {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int status = ZeekRecordCheck.run(new String[] {
            "--modbus", FIXTURES.resolve(MODBUS).toString(),
            "--s7comm", FIXTURES.resolve(S7COMM).toString()}, print(buffer));
        String out = buffer.toString(StandardCharsets.UTF_8);
        assertEquals(0, status, out);
        assertTrue(out.contains("48 records, 45 accepted, 3 rejected (3 known upstream parity, 0 unexpected)"), out);
        assertTrue(out.contains("84 records, 84 accepted, 0 rejected (0 known upstream parity, 0 unexpected)"), out);
        assertTrue(out.endsWith("RESULT: PASS" + System.lineSeparator()), out);
    }

    @Test
    void runFailsOnAnUnexpectedRejection(@TempDir Path dir) throws IOException {
        // An s7comm record with no uid: a real disagreement, not an upstream one.
        Path file = Files.writeString(dir.resolve("bad.jsonl"), "{\"ts\":1.0}\n");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        assertEquals(1, ZeekRecordCheck.run(new String[] {"--s7comm", file.toString()}, print(buffer)));
        assertTrue(buffer.toString(StandardCharsets.UTF_8).contains("UNEXPECTED"));
    }

    @Test
    void runReportsNoRecordsSeparatelyFromAPass(@TempDir Path dir) throws IOException {
        Path empty = Files.writeString(dir.resolve("empty.jsonl"), "");
        assertEquals(3, ZeekRecordCheck.run(new String[] {"--modbus", empty.toString()},
            print(new ByteArrayOutputStream())));
    }

    @Test
    void runRejectsBadArguments() {
        assertEquals(2, ZeekRecordCheck.run(new String[] {"--modbus"}, print(new ByteArrayOutputStream())));
        assertEquals(2, ZeekRecordCheck.run(new String[] {"--dnp3", "x"}, print(new ByteArrayOutputStream())));
        assertEquals(2, ZeekRecordCheck.run(new String[] {"--modbus", "/no/such/file"},
            print(new ByteArrayOutputStream())));
    }

    // The fixture's lines, in order.
    private static List<String> fixture(String name) throws IOException {
        return Files.readAllLines(FIXTURES.resolve(name), StandardCharsets.UTF_8);
    }

    // The Modbus function name a record carries.
    private static String funcOf(String line) {
        Matcher m = FUNC.matcher(line);
        return m.find() ? m.group(1) : "";
    }

    private static PrintStream print(ByteArrayOutputStream buffer) {
        return new PrintStream(buffer, true, StandardCharsets.UTF_8);
    }
}

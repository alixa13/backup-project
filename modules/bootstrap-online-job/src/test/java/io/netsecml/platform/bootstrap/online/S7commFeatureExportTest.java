package io.netsecml.platform.bootstrap.online;

import io.netsecml.platform.adapter.kafka.dto.ZeekS7commRecord;
import io.netsecml.platform.adapter.kafka.mapper.S7commEventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekS7commParser;
import io.netsecml.platform.application.usecase.S7commBuildFeaturesUseCase;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.S7commEvent;
import io.netsecml.platform.domain.event.SensorId;
import io.netsecml.platform.domain.feature.S7commConnectionState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// The exporter on REAL ICSNPP output (tests/fixtures/zeek/): one row per record,
// the use case's own vector per connection, the fresh flag, the client side,
// and rejects kept apart with their reason.
class S7commFeatureExportTest {

    private static final Path SAMPLE = Path.of("..", "..", "tests", "fixtures", "zeek",
        "icsnpp-s7comm-7ebeb03_s7comm.jsonl");

    private static List<String> sample() throws Exception {
        return Files.readAllLines(SAMPLE, StandardCharsets.UTF_8);
    }

    // The data rows of an export, split into fields (header dropped).
    private static List<String[]> rows(String csv) {
        List<String[]> out = new ArrayList<>();
        String[] lines = csv.split("\n");
        for (int i = 1; i < lines.length; i++) {
            out.add(lines[i].split(",", -1));
        }
        return out;
    }

    private static String export(List<String> lines, StringBuilder rejects) throws Exception {
        StringBuilder rows = new StringBuilder(String.join(",", S7commFeatureExport.COLUMNS)).append('\n');
        S7commFeatureExport.export("sample", lines, rows, rejects);
        return rows.toString();
    }

    @Test
    void theIcsnppSampleExportsOneRowPerRecord() throws Exception {
        StringBuilder rows = new StringBuilder();
        StringBuilder rejects = new StringBuilder();
        S7commFeatureExport.Summary s = S7commFeatureExport.export("sample", sample(), rows, rejects);
        assertEquals(84, s.rows());
        assertEquals(0, s.rejected());
        assertEquals(84, rows.toString().split("\n").length);
        assertEquals("", rejects.toString());
        assertEquals(28, S7commFeatureExport.COLUMNS.size(), "11 fields, 16 values, the flags");
    }

    // The values are exactly what S7commBuildFeaturesUseCase gives when fed each
    // uid's records in order, as S7commFeatureProcessFunction feeds them.
    @Test
    void eachRowCarriesTheUseCasesVectorForItsConnection() throws Exception {
        List<String[]> rows = rows(export(sample(), new StringBuilder()));
        S7commBuildFeaturesUseCase useCase = new S7commBuildFeaturesUseCase(Clock.systemUTC());
        Map<String, S7commConnectionState> states = new HashMap<>();
        int i = 0;
        for (String line : sample()) {
            MappingResult<ZeekS7commRecord> parsed = new JsonZeekS7commParser().parse(
                line.getBytes(StandardCharsets.UTF_8));
            NetworkEvent mapped = new S7commEventMapper().map(parsed.value(), new SensorId("x")).value();
            S7commEvent event = (S7commEvent) mapped;
            S7commConnectionState state = states.computeIfAbsent(event.connectionUid(),
                u -> S7commConnectionState.empty());
            float[] expected = useCase.build(event, state).vector().values();
            String[] row = rows.get(i++);
            for (int f = 0; f < 16; f++) {
                assertEquals(expected[f], Float.parseFloat(row[11 + f]), 0f, "row " + i + " f" + f);
            }
        }
    }

    @Test
    void freshMarksEachConnectionsFirstRowOnly() throws Exception {
        Set<String> seen = new HashSet<>();
        for (String[] row : rows(export(sample(), new StringBuilder()))) {
            assertEquals(seen.add(row[1]) ? "1" : "0", row[8], "uid " + row[1]);
        }
    }

    // A request goes to port 102, so its sender is the client; a response's
    // receiver is. Every row of one connection names the same client and server.
    @Test
    void theClientIsTheSideThatSendsToPort102() throws Exception {
        Map<String, String> clientOf = new HashMap<>();
        for (String[] row : rows(export(sample(), new StringBuilder()))) {
            String previous = clientOf.putIfAbsent(row[1], row[4] + ">" + row[5]);
            assertTrue(previous == null || previous.equals(row[4] + ">" + row[5]), "uid " + row[1]);
        }
    }

    @Test
    void aRecordThePipelineRejectsGoesToRejectsWithItsReason() throws Exception {
        List<String> lines = new ArrayList<>(sample());
        lines.add("{not json");
        StringBuilder rejects = new StringBuilder();
        S7commFeatureExport.Summary s = S7commFeatureExport.export("sample", lines, new StringBuilder(), rejects);
        assertEquals(84, s.rows());
        assertEquals(1, s.rejected());
        assertTrue(rejects.toString().contains("\"capture\":\"sample\""), rejects.toString());
        assertTrue(rejects.toString().contains("\"reason\""), rejects.toString());
    }

    @Test
    void runWritesTheHeaderAndEveryCapture(@TempDir Path dir) throws Exception {
        Path out = dir.resolve("f.csv");
        Path rejects = dir.resolve("r.jsonl");
        int status = S7commFeatureExport.run(new String[]{"--out", out.toString(), "--rejects", rejects.toString(),
            "a=" + SAMPLE, "b=" + SAMPLE}, new PrintStream(new ByteArrayOutputStream()));
        assertEquals(0, status);
        List<String> lines = Files.readAllLines(out, StandardCharsets.UTF_8);
        assertEquals(String.join(",", S7commFeatureExport.COLUMNS), lines.get(0));
        assertEquals(1 + 84 + 84, lines.size());
    }

    @Test
    void runRefusesBadArguments(@TempDir Path dir) throws Exception {
        PrintStream quiet = new PrintStream(new ByteArrayOutputStream());
        assertEquals(2, S7commFeatureExport.run(new String[]{}, quiet));
        assertEquals(2, S7commFeatureExport.run(new String[]{"--out", dir.resolve("f.csv").toString(), "--rejects",
            dir.resolve("r").toString(), "a=" + dir.resolve("missing.log")}, quiet));
        assertEquals(2, S7commFeatureExport.run(new String[]{"--out", dir.resolve("f.csv").toString(), "--rejects",
            dir.resolve("r").toString(), "no-equals-sign"}, quiet));
        Files.writeString(dir.resolve("empty.log"), "");
        assertEquals(3, S7commFeatureExport.run(new String[]{"--out", dir.resolve("f.csv").toString(), "--rejects",
            dir.resolve("r").toString(), "a=" + dir.resolve("empty.log")}, quiet));
    }
}

package io.netsecml.platform.domain.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import static org.junit.jupiter.api.Assertions.*;

class ZeekConnFixturesReadableTest {
    private static final Path FIXTURES = Paths.get("..", "..", "tests", "fixtures", "zeek_conn");

    @Test
    void validFixturesParseAsJsonObjects() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        for (String name : new String[]{"valid-tcp-ssl.json", "valid-udp-dns.json"}) {
            byte[] bytes = Files.readAllBytes(FIXTURES.resolve(name));
            assertTrue(mapper.readTree(bytes).isObject(), name + " must parse as a JSON object");
        }
    }

    @Test
    void malformedFixtureIsNotValidJson() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        byte[] bytes = Files.readAllBytes(FIXTURES.resolve("malformed.json"));
        assertThrows(Exception.class, () -> mapper.readTree(bytes));
    }
}

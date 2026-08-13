package io.netsecml.platform.domain.event;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ServiceCodeTest {
    @Test
    void mapsKnownValuesCaseInsensitively() {
        assertEquals(ServiceCode.DNS, ServiceCode.fromZeekValue("dns"));
        assertEquals(ServiceCode.HTTP, ServiceCode.fromZeekValue("HTTP"));
        assertEquals(ServiceCode.SSL, ServiceCode.fromZeekValue("ssl"));
    }

    @Test
    void mapsMissingOrUnknownToUnknown() {
        assertEquals(ServiceCode.UNKNOWN, ServiceCode.fromZeekValue(null));
        assertEquals(ServiceCode.UNKNOWN, ServiceCode.fromZeekValue(""));
        assertEquals(ServiceCode.UNKNOWN, ServiceCode.fromZeekValue("ftp"));
    }
}

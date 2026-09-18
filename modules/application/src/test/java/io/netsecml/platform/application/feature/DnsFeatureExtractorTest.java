package io.netsecml.platform.application.feature;

import io.netsecml.platform.domain.event.DnsEvent;
import io.netsecml.platform.domain.event.DnsQType;
import io.netsecml.platform.domain.event.DnsQuery;
import io.netsecml.platform.domain.event.DnsRcode;
import io.netsecml.platform.domain.event.DnsResponse;
import io.netsecml.platform.domain.event.EventEnvelope;
import io.netsecml.platform.domain.event.EventId;
import io.netsecml.platform.domain.event.LogType;
import io.netsecml.platform.domain.event.SensorId;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class DnsFeatureExtractorTest {
    private final DnsFeatureExtractor extractor = new DnsFeatureExtractor();
    private static final SensorId SENSOR = new SensorId("sensor-eu-1");

    // Mirrors DnsEventTest's fixture pattern. enrichment is irrelevant to this
    // extractor (it belongs to the common tier, a different task), so it is
    // always null here. qtypeCode is a separate parameter, not qtype.code(),
    // specifically so a caller CAN construct the unenumerated case (qtype ==
    // OTHER, qtypeCode == some real IANA number OTHER does not name) -- the
    // exact shape unenumeratedQtypeEmitsItsRawIanaNumberNotNegativeOne below
    // needs and every enumerated-code call site below supplies qtype.code() for.
    private DnsEvent dnsEvent(String qname, DnsQType qtype, int qtypeCode, int transId, DnsResponse response) {
        EventEnvelope envelope = new EventEnvelope(
            EventId.derive(SENSOR, "abc"), Instant.now(), SENSOR, LogType.DNS, "abc");
        DnsQuery query = new DnsQuery(qname, qtype, transId, qtypeCode);
        return new DnsEvent(envelope, query, response, "10.0.0.5", true, null);
    }

    // response is nullable by design: dns.log records a query that got no answer.
    // Every response-derived feature defaults to zero, and every query-derived one
    // must still be computed -- if a missing response zeroed the qname features
    // too, the unanswered queries a DGA generates would look identical to each
    // other regardless of the name asked for, which is the signal.
    //
    // This asserts every one of the twelve indices, not just qname_length and
    // entropy: the name promises "every" feature, and dns_qtype (1), dns_label_count
    // (9), dns_digit_ratio (10) and dns_hyphen_ratio (11) are just as much a part
    // of that claim as the two the brief's own snippet checked, and just as able
    // to hide an index transposition if left unchecked.
    @Test
    void anUnansweredQueryStillCarriesEveryQnameFeature() {
        DnsEvent event = dnsEvent("x7q2m9v4z1kd.com", DnsQType.A, DnsQType.A.code(), 4242, null);

        float[] values = extractor.extractProtocolTier(event);

        assertEquals(12, values.length);

        // Response-derived indices all default to zero -- there is no response
        // to read rcode, the three flags, answer count or ttl from.
        assertEquals(0f, values[0], "dns_rcode defaults to 0 with no response");
        assertEquals(0f, values[2], "dns_authoritative defaults to 0 with no response");
        assertEquals(0f, values[3], "dns_recursion_available defaults to 0 with no response");
        assertEquals(0f, values[4], "dns_truncated defaults to 0 with no response");
        assertEquals(0f, values[5], "dns_answer_count defaults to 0 with no response");
        assertEquals(0f, values[6], "dns_ttl defaults to 0 with no response");

        // Query-derived indices are all still computed. "x7q2m9v4z1kd.com" is 16
        // characters, 2 labels, 5 digits (7,2,9,4,1) and 0 hyphens.
        assertEquals(1f, values[1], "dns_qtype is computed from the query regardless (A = 1)");
        assertEquals(16f, values[7], "dns_qname_length is computed from the query regardless");
        assertTrue(values[8] > 0f, "dns_qname_entropy is computed from the query regardless");
        assertEquals(2f, values[9], "dns_label_count is computed from the query regardless");
        assertEquals(5f / 16f, values[10], 1e-6f, "dns_digit_ratio is computed from the query regardless");
        assertEquals(0f, values[11], "dns_hyphen_ratio is computed from the query regardless (no hyphens present)");
    }

    // Full-array pin against index transposition. The non-boolean values are
    // chosen pairwise distinct (rcode=3, qtype=28, answerCount=7, ttl=300,
    // qnameLength=11, labelCount=2, and two different ratios) so that swapping
    // any two of them -- not merely computing one wrongly -- produces a visible
    // mismatch.
    //
    // It canNOT catch a swap between indices 2 and 4, because both are booleans
    // set true and a boolean carries only 0f or 1f: no choice of fixture makes
    // two true flags distinguishable by value. That gap is closed by
    // authoritativeRecursionAvailableAndTruncatedMapToDistinctIndices below,
    // which sets one flag at a time so each position is identified alone.
    // Neither test covers the booleans on its own; the pair does.
    //
    // It is not meant to be a realistic Zeek response -- NXDOMAIN with seven
    // answers does not occur on real traffic.
    @Test
    void respondedQueryPopulatesAllTwelveIndicesInSchemaOrder() {
        String qname = "ab12-cd.com";
        DnsResponse response = new DnsResponse(DnsRcode.NXDOMAIN, true, false, true, 7, 300L, DnsRcode.NXDOMAIN.code());
        DnsEvent event = dnsEvent(qname, DnsQType.AAAA, DnsQType.AAAA.code(), 7, response);

        float[] values = extractor.extractProtocolTier(event);

        assertArrayEquals(new float[]{
            3f,                                          // 0  dns_rcode = NXDOMAIN
            28f,                                         // 1  dns_qtype = AAAA
            1f,                                           // 2  dns_authoritative = true
            0f,                                           // 3  dns_recursion_available = false
            1f,                                           // 4  dns_truncated = true
            7f,                                           // 5  dns_answer_count
            300f,                                         // 6  dns_ttl
            11f,                                          // 7  dns_qname_length ("ab12-cd.com")
            (float) QnameFeatures.shannonEntropy(qname),  // 8  dns_qname_entropy
            2f,                                            // 9  dns_label_count ("ab12-cd", "com")
            2f / 11f,                                      // 10 dns_digit_ratio ('1','2' of 11 chars)
            1f / 11f                                       // 11 dns_hyphen_ratio ('-' of 11 chars)
        }, values, 1e-6f);
    }

    // dns_authoritative, dns_recursion_available and dns_truncated are three
    // independent booleans at three separate indices (2, 3, 4). Booleans only
    // take two values, so a fixture with two flags sharing a value (as in
    // respondedQueryPopulatesAllTwelveIndicesInSchemaOrder, where authoritative
    // and truncated are both true) cannot alone catch a transposition between
    // those two indices. This toggles exactly one flag at a time so each index
    // is pinned independently of the other two.
    @Test
    void authoritativeRecursionAvailableAndTruncatedMapToDistinctIndices() {
        DnsResponse onlyAuthoritative =
            new DnsResponse(DnsRcode.NOERROR, true, false, false, 0, 0L, DnsRcode.NOERROR.code());
        DnsResponse onlyRecursionAvailable =
            new DnsResponse(DnsRcode.NOERROR, false, true, false, 0, 0L, DnsRcode.NOERROR.code());
        DnsResponse onlyTruncated =
            new DnsResponse(DnsRcode.NOERROR, false, false, true, 0, 0L, DnsRcode.NOERROR.code());

        float[] a = extractor.extractProtocolTier(
            dnsEvent("example.com", DnsQType.A, DnsQType.A.code(), 1, onlyAuthoritative));
        float[] b = extractor.extractProtocolTier(
            dnsEvent("example.com", DnsQType.A, DnsQType.A.code(), 1, onlyRecursionAvailable));
        float[] c = extractor.extractProtocolTier(
            dnsEvent("example.com", DnsQType.A, DnsQType.A.code(), 1, onlyTruncated));

        assertArrayEquals(new float[]{1f, 0f, 0f}, new float[]{a[2], a[3], a[4]}, 0f,
            "authoritative alone sets only index 2");
        assertArrayEquals(new float[]{0f, 1f, 0f}, new float[]{b[2], b[3], b[4]}, 0f,
            "recursionAvailable alone sets only index 3");
        assertArrayEquals(new float[]{0f, 0f, 1f}, new float[]{c[2], c[3], c[4]}, 0f,
            "truncated alone sets only index 4");
    }

    // FEATURE_COUNT is what DnsBuildFeaturesUseCase's constructor checks the
    // registered schema against instead of a literal 12. If this array's actual
    // length ever drifted from the constant, every other test in this file would
    // still pass (they all index into the real array, never the constant), so
    // this is the only test that would catch that drift.
    @Test
    void featureCountConstantMatchesTheLengthOfTheArrayItDescribes() {
        DnsEvent event = dnsEvent("example.com", DnsQType.A, DnsQType.A.code(), 1, null);

        assertEquals(DnsFeatureExtractor.FEATURE_COUNT, extractor.extractProtocolTier(event).length);
    }

    // contracts/features/dns-feature-schema-v1.json index 13 (dns_qtype, local
    // index 1 here) promises "the IANA QTYPE number from qtype" -- the actual
    // registry number, not DnsQType's ten-value subset of it. QTYPE 65
    // (HTTPS/SVCB) is real and growing traffic (Apple and Chrome resolvers)
    // that DnsQType does not enumerate, so DnsQType.fromCode(65) correctly
    // returns OTHER. Before this fix the extractor read query.qtype().code(),
    // which is OTHER's own placeholder (-1) -- silently contradicting the
    // contract's own text for every one of those records. query.qtypeCode() is
    // the fix: the wire number itself, carried through unchanged.
    @Test
    void anUnenumeratedQtypeEmitsItsRawIanaNumberNotNegativeOne() {
        DnsEvent event = dnsEvent("example.com", DnsQType.OTHER, 65, 1, null);

        float[] values = extractor.extractProtocolTier(event);

        assertEquals(65f, values[1], "dns_qtype must carry qtype 65 itself, not OTHER's -1 fallback");
    }

    // Same defect, same fix, as the qtype case above, for index 12 (dns_rcode,
    // local index 0): RCODE 16 (BADVERS/BADSIG) is real and not one of
    // DnsRcode's six enumerated values, so DnsRcode.fromCode(16) correctly
    // returns OTHER. response.rcodeCode() carries 16 through where
    // response.rcode().code() would have emitted -1.
    @Test
    void anUnenumeratedRcodeEmitsItsRawIanaNumberNotNegativeOne() {
        DnsResponse response = new DnsResponse(DnsRcode.fromCode(16), false, false, false, 0, 0L, 16);
        DnsEvent event = dnsEvent("example.com", DnsQType.A, DnsQType.A.code(), 1, response);

        float[] values = extractor.extractProtocolTier(event);

        assertEquals(16f, values[0], "dns_rcode must carry rcode 16 itself, not OTHER's -1 fallback");
    }

    // The enumerated branch, kept alongside the two unenumerated cases above so
    // both are visibly covered side by side: for a code DnsQType DOES name,
    // qtypeCode and qtype.code() agree, and the emitted value is still the
    // plain IANA number (A = 1) -- respondedQueryPopulatesAllTwelveIndicesInSchemaOrder
    // and anUnansweredQueryStillCarriesEveryQnameFeature above already pin this
    // same behaviour for qtype 1 and 28, unchanged by this fix; this test names
    // it explicitly as the enumerated counterpart to the two tests above.
    @Test
    void anEnumeratedQtypeEmitsItsPlainIanaNumber() {
        DnsEvent event = dnsEvent("example.com", DnsQType.A, DnsQType.A.code(), 1, null);

        float[] values = extractor.extractProtocolTier(event);

        assertEquals(1f, values[1], "dns_qtype = A's IANA number");
    }
}

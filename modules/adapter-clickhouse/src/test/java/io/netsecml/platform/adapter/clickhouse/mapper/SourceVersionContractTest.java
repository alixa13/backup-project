package io.netsecml.platform.adapter.clickhouse.mapper;

import io.netsecml.platform.adapter.clickhouse.ClickHouseTestSupport;
import io.netsecml.platform.domain.event.LogType;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import static org.junit.jupiter.api.Assertions.assertTrue;

// InvalidEventRowMapper.sourceVersion() reconstructs a real contract id --
// "zeek-" + logType.wireName() + "-source-v1" -- from the constructed log type
// rather than reading it from a file, so that it stays correct as more Zeek log
// types are added. That derivation is only trustworthy if a contract file with
// the derived id actually exists on disk for every LogType constant: contracts
// version by creating a new "-v2" file rather than editing one in place, so the
// day the derivation and the contracts directory disagree, source_version would
// silently start recording an id with no file behind it. This test needs no
// Docker and no Jackson -- plain file existence is enough to catch that drift.
class SourceVersionContractTest {

    @Test
    void everyLogTypeHasAMatchingSourceContractFile() {
        for (LogType logType : LogType.values()) {
            String sourceVersion = new InvalidEventRowMapper(logType).sourceVersion();
            Path contractPath = ClickHouseTestSupport.repoPath("contracts", "source", sourceVersion + ".json");

            assertTrue(Files.exists(contractPath),
                "LogType." + logType.name() + " derives source_version \"" + sourceVersion
                + "\", but " + contractPath + " does not exist -- the derivation in "
                + "InvalidEventRowMapper.sourceVersion() has drifted from the contracts directory");
        }
    }
}

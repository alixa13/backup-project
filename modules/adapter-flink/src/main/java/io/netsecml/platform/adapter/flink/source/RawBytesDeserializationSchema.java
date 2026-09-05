package io.netsecml.platform.adapter.flink.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

// Pass-through deserializer: the Kafka source hands the pipeline raw bytes and
// each job decides how to interpret them. Shared by the online and archive jobs
// so the same nine lines do not exist in two composition roots.
public final class RawBytesDeserializationSchema implements DeserializationSchema<byte[]> {

    @Override
    public byte[] deserialize(byte[] message) {
        return message;
    }

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(byte[].class);
    }
}

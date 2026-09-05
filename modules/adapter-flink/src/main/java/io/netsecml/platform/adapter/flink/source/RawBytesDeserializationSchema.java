package io.netsecml.platform.adapter.flink.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

// Pass-through deserializer: the Kafka source hands the pipeline raw bytes and
// each job decides how to interpret them. Shared by the online and archive jobs
// so the same nine lines do not exist in two composition roots.
public final class RawBytesDeserializationSchema implements DeserializationSchema<byte[]> {

    // Identity: the raw Kafka value, untouched. Interpretation is the caller's job.
    @Override
    public byte[] deserialize(byte[] message) {
        return message;
    }

    // Never signals end-of-stream: a Kafka source is unbounded from this
    // deserializer's point of view, no matter which job reads it.
    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    // Tells Flink the produced type is a plain byte array, since there is no
    // POJO/generic signature here for type extraction to infer it from.
    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(byte[].class);
    }
}

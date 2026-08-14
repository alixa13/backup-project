package io.netsecml.platform.adapter.flink.process;

import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.adapter.kafka.mapper.EventMapper;
import io.netsecml.platform.adapter.kafka.parser.JsonZeekConnParser;
import io.netsecml.platform.domain.event.MappingResult;
import io.netsecml.platform.domain.event.NetworkEvent;
import io.netsecml.platform.domain.event.SensorId;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public final class ParseMapValidateFunction extends ProcessFunction<byte[], NetworkEvent> {
    public static final OutputTag<RejectedRecord> REJECTED_TAG =
        new OutputTag<RejectedRecord>("rejected") {};

    private final SensorId sensor;
    private transient JsonZeekConnParser parser;
    private transient EventMapper mapper;

    public ParseMapValidateFunction(SensorId sensor) {
        this.sensor = sensor;
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
        super.open(openContext);
        parser = new JsonZeekConnParser();
        mapper = new EventMapper();
    }

    @Override
    public void processElement(byte[] rawPayload, Context ctx, Collector<NetworkEvent> out) {
        MappingResult<ZeekConnEvent> parsed = parser.parse(rawPayload);
        if (!parsed.isValid()) {
            ctx.output(REJECTED_TAG, new RejectedRecord(rawPayload, parsed.reason(), parsed.detail()));
            return;
        }

        MappingResult<NetworkEvent> mapped = mapper.map(parsed.value(), sensor);
        if (!mapped.isValid()) {
            ctx.output(REJECTED_TAG, new RejectedRecord(rawPayload, mapped.reason(), mapped.detail()));
            return;
        }

        out.collect(mapped.value());
    }
}

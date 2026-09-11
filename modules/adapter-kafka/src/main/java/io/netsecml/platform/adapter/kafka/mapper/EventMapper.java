package io.netsecml.platform.adapter.kafka.mapper;

import io.netsecml.platform.adapter.kafka.dto.ZeekConnEvent;
import io.netsecml.platform.domain.event.*;
import java.time.Instant;

public final class EventMapper {
    public MappingResult<NetworkEvent> map(ZeekConnEvent dto, SensorId sensor) {
        if (dto.id() == null || dto.id().isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "id is required");
        }
        if (Double.isNaN(dto.ts()) || dto.ts() < 0) {
            return MappingResult.invalid(ReasonCode.INVALID_TIMESTAMP, "ts must be a non-negative number, was " + dto.ts());
        }
        if (dto.idOrigH() == null || dto.idOrigH().isBlank()
                || dto.idRespH() == null || dto.idRespH().isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "id_orig_h and id_resp_h are required");
        }
        if (dto.connState() == null || dto.connState().isBlank()) {
            return MappingResult.invalid(ReasonCode.MISSING_REQUIRED_FIELD, "conn_state is required");
        }

        EventId eventId = EventId.derive(sensor, dto.id());
        Instant eventTime = Instant.ofEpochMilli(Math.round(dto.ts() * 1000.0));

        // This mapper handles conn.log exclusively, so LogType.CONN is a fixed
        // constant here rather than something derived per event.
        LogType logType = LogType.CONN;

        // dto.id() is Zeek's uid: the source contract's upstream record id and the
        // cross-protocol correlation key. eventId above already derives from this
        // same value, and that derivation is unchanged — one conn record exists per
        // connection, so sensor:uid remains a safe identity for eventId even though
        // connectionUid itself is never anything more than a correlation key.
        String connectionUid = dto.id();

        ConnectionTuple tuple;
        try {
            tuple = new ConnectionTuple(
                dto.idOrigH(), dto.idOrigP(), dto.idRespH(), dto.idRespP(),
                Protocol.fromZeekValue(dto.proto()),
                ServiceCode.fromZeekValue(dto.service()),
                ConnectionState.fromZeekValue(dto.connState()));
        } catch (IllegalArgumentException e) {
            return MappingResult.invalid(ReasonCode.INVALID_PORT, e.getMessage());
        }

        long durationMillis = dto.duration() == null ? 0L : Math.round(dto.duration() * 1000.0);
        long originBytes = dto.origBytes() == null ? 0L : dto.origBytes();
        long responseBytes = dto.respBytes() == null ? 0L : dto.respBytes();
        int originPackets = dto.origPkts() == null ? 0 : dto.origPkts().intValue();
        int responsePackets = dto.respPkts() == null ? 0 : dto.respPkts().intValue();
        long missedBytes = dto.missedBytes() == null ? 0L : dto.missedBytes();

        ConnectionMeasurements measurements;
        try {
            measurements = new ConnectionMeasurements(
                durationMillis, originBytes, responseBytes, originPackets, responsePackets, missedBytes);
        } catch (IllegalArgumentException e) {
            return MappingResult.invalid(ReasonCode.INVALID_COUNTER, e.getMessage());
        }

        ConnectionLocality locality = new ConnectionLocality(dto.localOrig(), dto.localResp());

        // This mapper handles conn.log exclusively, so it always produces a
        // ConnEvent -- the envelope carries identity and timing, ConnEvent carries
        // the conn-specific payload this DTO parsed.
        NetworkEvent event = new ConnEvent(new EventEnvelope(eventId, eventTime, sensor, logType, connectionUid),
            tuple, measurements, locality);
        return MappingResult.valid(event);
    }
}

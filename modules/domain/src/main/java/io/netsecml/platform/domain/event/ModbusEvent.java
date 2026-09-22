package io.netsecml.platform.domain.event;

import java.util.Objects;

// A modbus_detailed.log record (ICSNPP's icsnpp-modbus package, not Zeek's own
// bare modbus.log -- see ModbusFeatureSchemaV1's javadoc for why only the
// detailed log can supply all 42 frozen features): the shared envelope plus
// the request/response fields the frozen upstream contract reads.
//
// sourceIp and destinationIp are components of THIS record, not of
// EventEnvelope, for the same reason DnsEvent carries its own sourceIp:
// EventEnvelope is (eventId, eventTime, sensor, logType, connectionUid) --
// no addresses at all. Modbus needs BOTH endpoints, not just one, because the
// causal engine's state key normalizes them into client/server roles
// (docs/superpowers/specs/2026-09-21-modbus-stage1-design.md section 7):
//
//   client_ip = (direction == request) ? sourceIp : destinationIp
//   server_ip = (direction == request) ? destinationIp : sourceIp
//
// That normalization is a later task's job (ModbusEntityKey); this record
// only has to carry both addresses for it to read.
//
// direction is a nested enum, not a boolean "isRequest": the upstream
// contract's own direction normalization
// (docs/superpowers/specs/2026-09-21-modbus-stage1-design.md section 5) is a
// validation step, not a coercion -- an unresolvable direction is a DLQ
// rejection, never a guess -- and a two-value enum makes an impossible third
// state unrepresentable, which a boolean cannot.
//
// address and quantity are boxed Double, not primitive double, because their
// absence is itself meaningful: the frozen schema has a separate
// address_present / quantity_present mask feature
// (ModbusFeatureSchemaV1, indices 8-11) precisely so a genuine address of 0
// can be told apart from "no address field on this record". A primitive with
// a sentinel (e.g. -1 or 0) would make that distinction unrepresentable at
// the type level; null does not collide with any real address or quantity.
//
// requestValues and responseValues are the record's only array components,
// so -- per this codebase's rule for every record with an array component --
// they are defensively copied both in the compact constructor (so a caller's
// later mutation of the array it passed in cannot reach this record) and in
// the accessor (so a caller reading requestValues()/responseValues() cannot
// mutate this record's internal copy). Both accessors are overridden below
// for exactly that reason.
//
// No parser or mapper exists yet: this record only becomes reachable once
// both land (see the class-level note on NetworkEvent.permits below).
public record ModbusEvent(EventEnvelope envelope, ModbusDirection direction, String sourceIp,
                          String destinationIp, int functionCode, String transactionId, String unitId,
                          Double address, Double quantity, boolean matched, double[] requestValues,
                          double[] responseValues)
        implements NetworkEvent {

    // Direction of a single modbus_detailed.log record: ICSNPP logs a request
    // and its matching response as two separate records sharing one
    // transaction_id, never as one combined record -- which is exactly why
    // event identity needs both a direction and a timestamp component (see
    // this class's javadoc and
    // docs/superpowers/specs/2026-09-21-modbus-stage1-design.md section 6):
    // tid alone cannot tell a request from its own response.
    public enum ModbusDirection {
        REQUEST, RESPONSE
    }

    public ModbusEvent {
        Objects.requireNonNull(envelope, "envelope must not be null");
        Objects.requireNonNull(direction, "direction must not be null");

        // Both endpoints are structural here, unlike EventEnvelope's optional
        // connectionUid: the causal engine's state key is built from both
        // (see class javadoc), so a record missing either address cannot be
        // keyed at all, let alone scored.
        if (sourceIp == null || sourceIp.isBlank()) {
            throw new IllegalArgumentException("sourceIp must not be blank");
        }
        if (destinationIp == null || destinationIp.isBlank()) {
            throw new IllegalArgumentException("destinationIp must not be blank");
        }

        // transaction_id is required upstream (the pending-TID machine and
        // event identity both depend on it -- section 6 and section 7 of the
        // design doc); unit_id is required too, but the design doc records
        // that the upstream engine falls back to a literal "NA" String for an
        // absent unit rather than dropping the record (section 7). Both are
        // therefore non-null Strings here: a caller representing "absent
        // unit" passes the same "NA" sentinel the engine does, not null.
        Objects.requireNonNull(transactionId, "transactionId must not be null");
        Objects.requireNonNull(unitId, "unitId must not be null");

        // Defensive copy #1 of 2: protects this record from a mutation the
        // caller makes to the array it passed in, AFTER construction.
        requestValues = Objects.requireNonNull(requestValues, "requestValues must not be null; "
            + "pass an empty array for \"no numeric request values on this record\", not null").clone();
        responseValues = Objects.requireNonNull(responseValues, "responseValues must not be null; "
            + "pass an empty array for \"no numeric response values on this record\", not null").clone();
    }

    // Defensive copy #2 of 2: without this override, the record's generated
    // accessor would hand out the constructor's own internal array by
    // reference, and a caller mutating what it got back would silently
    // corrupt this supposedly-immutable record.
    @Override
    public double[] requestValues() {
        return requestValues.clone();
    }

    // Same reasoning as requestValues() above, for the response side.
    @Override
    public double[] responseValues() {
        return responseValues.clone();
    }
}

##! netsec-ml sensor policy (design §4): ICSNPP modbus_detailed and s7comm
##! records to their Kafka topics as bare JSON objects; nothing to local disk.

@load packages

module NetSec;

export {
    ## Kafka topic for ICSNPP modbus_detailed records (one per packet).
    const modbus_topic = "netsec.modbus.raw.v1" &redef;
    ## Kafka topic for ICSNPP s7comm records (one per packet).
    const s7comm_topic = "netsec.s7comm.raw.v1" &redef;
}

# The connection tuple is written id_orig_h, not id.orig_h: the spelling this
# platform's sensor has always used (both parsers also accept the dotted form).
redef Log::default_scope_sep = "_";

# No local log files: every stream's default filter writes nowhere from the
# first record on (this also catches packet_filter.log, which is written
# before any zeek_init handler below can run).
redef Log::default_writer = Log::WRITER_NONE;

# Each Kafka message is the bare record with epoch-second timestamps -- no
# log-name wrapper, which the parsers would reject.
redef Kafka::topic_name = "";
redef Kafka::tag_json = F;
redef Kafka::json_timestamps = JSON::TS_EPOCH;

event zeek_init() &priority=-10
    {
    # Only the two OT logs go to Kafka, each to its own topic.
    Log::add_filter(Modbus_Extended::LOG_DETAILED, [$name="netsec-kafka-modbus",
        $writer=Log::WRITER_KAFKAWRITER, $path="modbus_detailed",
        $config=table(["topic_name"] = modbus_topic)]);
    Log::add_filter(S7COMM::LOG_S7COMM, [$name="netsec-kafka-s7comm",
        $writer=Log::WRITER_KAFKAWRITER, $path="s7comm",
        $config=table(["topic_name"] = s7comm_topic)]);

    # The default filters now only feed the no-op writer; dropping them saves
    # formatting every other log's records for nothing.
    for ( id in Log::active_streams )
        Log::remove_filter(id, "default");
    }

##! Check policy: the same packages and field naming as netsec.zeek, but JSON
##! log files in the working directory instead of Kafka. Used only by
##! deploy.sh zeek-check and deploy/tests/zeek-fixtures.sh. zeek-kafka formats
##! its messages with this same JSON formatter, so these files are exactly
##! what the sensor sends.

@load packages

redef Log::default_scope_sep = "_";
redef LogAscii::use_json = T;
redef LogAscii::json_timestamps = JSON::TS_EPOCH;

@load Seiso/Kafka
@load base/protocols/conn

redef Kafka::topic_name = "";
redef Kafka::tag_json = T;

event zeek_init() &priority=-10
{
    # handles HTTP
    local http_filter: Log::Filter = [
        $name = "kafka-http",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(
                ["metadata.broker.list"] = "kafka:9092"
        ),
        $path = "zeek-http"
    ];
    Log::add_filter(HTTP::LOG, http_filter);

    # handles DNS
    local dns_filter: Log::Filter = [
        $name = "kafka-dns",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(
                ["metadata.broker.list"] = "kafka:9092"
        ),
        $path = "zeek-dns"
    ];
    Log::add_filter(DNS::LOG, dns_filter);

    # handles SSL
    local ssl_filter: Log::Filter = [
        $name = "kafka-ssl",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(
                ["metadata.broker.list"] = "kafka:9092"
        ),
        $path = "zeek-ssl"
    ];
    Log::add_filter(SSL::LOG, ssl_filter);

    # handles Connection logs
    local conn_filter: Log::Filter = [
        $name = "kafka-conn",
        $writer = Log::WRITER_KAFKAWRITER,
        $config = table(
                ["metadata.broker.list"] = "kafka:9092"
        ),
        $path = "zeek-conn"
    ];
    Log::add_filter(Conn::LOG, conn_filter);

}
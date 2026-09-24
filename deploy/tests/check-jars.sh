#!/usr/bin/env bash
# Asserts each shaded job JAR (design §6) carries what the Flink 2.2.1 image
# lacks -- the Kafka connector and client, zstd, Jackson, our own modules, and
# for the archive job the ClickHouse client -- and none of what flink-dist
# already ships: two copies of Flink's runtime, Kryo or commons-* on one
# classpath break class loading. Run after './mvnw install -DskipTests'.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/lib.sh"
REPO="$(cd "$HERE/../.." && pwd)"

# yes/no: does the JAR listing in $1 contain the entry $2?
has() { grep -qxF "$2" <<< "$1" && echo yes || echo no; }

# Classes flink-dist-2.2.1.jar already ships (checked against the image).
PROVIDED=(
  org/apache/flink/streaming/api/environment/StreamExecutionEnvironment.class
  org/apache/flink/runtime/jobgraph/JobGraph.class
  org/apache/flink/client/cli/CliFrontend.class
  org/apache/flink/connector/base/DeliveryGuarantee.class
  com/esotericsoftware/kryo/Kryo.class
  org/objenesis/Objenesis.class
  org/apache/commons/lang3/StringUtils.class
  net/jpountz/lz4/LZ4Factory.class
  org/xerial/snappy/Snappy.class
  org/slf4j/Logger.class
)

# Needed by both jobs at run time and absent from the Flink image.
NEEDED=(
  org/apache/flink/connector/kafka/source/KafkaSource.class
  org/apache/kafka/clients/producer/KafkaProducer.class
  com/github/luben/zstd/Zstd.class
  com/fasterxml/jackson/databind/ObjectMapper.class
  io/netsecml/platform/domain/event/S7commEvent.class
)

for job in online archive; do
  # The shaded JAR carries the classifier "all".
  module="bootstrap-${job}-job"
  jars=("${REPO}/modules/${module}/target/${module}"-*-all.jar)
  jar="${jars[0]}"
  [ -f "$jar" ] || jar=""
  assert_eq yes "$([ -n "$jar" ] && [ -f "$jar" ] && echo yes || echo no)" "${job}: shaded JAR exists"
  [ -n "$jar" ] || continue
  entries="$(unzip -Z1 "$jar")"

  # What the image lacks must be inside; what it ships must not be.
  for c in "${NEEDED[@]}"; do assert_eq yes "$(has "$entries" "$c")" "${job} JAR contains ${c}"; done
  for c in "${PROVIDED[@]}"; do assert_eq no "$(has "$entries" "$c")" "${job} JAR lacks ${c}"; done

  # The manifest names the job, so the submitter's -c is a belt, not a brace.
  main="$(unzip -p "$jar" META-INF/MANIFEST.MF | sed -n 's/^Main-Class: *//p' | tr -d '\r')"
  case "$job" in
    online)
      assert_eq io.netsecml.platform.bootstrap.online.OnlineFeatureJob "$main" "online Main-Class"
      assert_eq yes "$(has "$entries" io/netsecml/platform/adapter/kafka/parser/JsonZeekModbusParser.class)" "online JAR contains the Modbus parser"
      assert_eq yes "$(has "$entries" io/netsecml/platform/bootstrap/online/ZeekRecordCheck.class)" "online JAR contains ZeekRecordCheck"
      ;;
    archive)
      assert_eq io.netsecml.platform.bootstrap.archive.ArchiveJob "$main" "archive Main-Class"
      assert_eq yes "$(has "$entries" com/clickhouse/client/api/Client.class)" "archive JAR contains the ClickHouse client"
      assert_eq yes "$(has "$entries" org/apache/hc/client5/http/impl/classic/CloseableHttpClient.class)" "archive JAR contains httpclient5"
      ;;
  esac
done

finish

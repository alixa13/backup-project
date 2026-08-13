# infrastructure/kafka/

`topics/internal-topics.yaml`: declarative definitions for the internal, versioned
Kafka topics this platform owns (feature, prediction, alert, DLQ, invalid-event).
Does not define the external `conn` source topic, which is owned upstream. Not yet
committed — Step 1 scope is directory skeleton only.

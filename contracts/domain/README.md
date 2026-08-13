# contracts/domain/

Owns `network-event-v1.json`: the normalized domain event shape (event ID/time,
typed connection tuple/measurements/locality). No Kafka DTO fields leak into this
contract. Frozen Day 2 (Roadmap.md Section 5). Not yet committed — Step 1 scope is
directory skeleton only.

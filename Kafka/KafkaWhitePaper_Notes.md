# Kafka White Paper Notes (LinkedIn, 2011)

> Paper: *Kafka: a Distributed Messaging System for Log Processing* — Jay Kreps, Neha Narkhede, Jun Rao (LinkedIn, NetDB'11).
> This is a learner-friendly walkthrough of the original Kafka white paper. It mirrors the short-bullet style of `KafkaArchitectureWhitePaper.docx` and fills in the parts that doc doesn't cover.

> **How to read this doc:**
> - The body explains Kafka **as the 2011 paper described it**.
> - Anything that has changed in modern Kafka (≈ Kafka 4.x, 2025) is flagged with a callout like this:
>   > **[Modern Kafka — 2025]** what's different today.
> - A summary of all changes lives in **Section 11 — What's new since 2011**.

---

## 1. Why Kafka was built

- Internet companies generate **two kinds of "log" data**:
  - **User activity events** — logins, page views, clicks, likes, shares, comments, search queries.
  - **Operational metrics** — service call stack, latency, errors, CPU/memory/network/disk.
- Old use case: feed this data into analytics for offline reporting.
- New use case (the reason Kafka exists): feed it into **live product features** — search relevance, recommendations, ad targeting, abuse/spam detection, newsfeed.
- Problem: this "log" stream is **orders of magnitude bigger** than the "real" data.
  - Example: clicks generate one event per click, *plus* events for the dozens of items shown but not clicked.
  - Facebook ≈ 6 TB user-activity events / day. China Mobile ≈ 5–8 TB call records / day.
- Existing tools fell short → LinkedIn built Kafka to combine **messaging system** + **log aggregator** in one box.

---

## 2. Why existing systems didn't fit

### Traditional enterprise messaging (JMS, IBM WebSphere MQ, TIBCO, Oracle EMS)
- Too feature-heavy: transactional inserts, per-message acks, out-of-order acks.
  - For logs you *don't care* if a pageview event is lost occasionally.
- Throughput is not the primary goal — no producer-side batching API → every message = one TCP roundtrip.
- Weak distributed support — no easy way to partition messages across machines.
- Assume messages are consumed almost immediately → performance tanks when messages pile up (which is normal for offline / data-warehouse consumers).

### Log aggregators (Scribe, Yahoo Data Highway, Flume)
- Built for **offline** consumption only — they dump to HDFS / NFS.
- Leak implementation details to consumers (e.g. "minute files").
- Use a **push** model — broker pushes to consumer.
  - Kafka prefers **pull** → each consumer reads at its own max rate, can't be flooded, and can **rewind**.

### HedWig (Yahoo Research pub/sub)
- Scalable + strong durability, but designed for storing a data-store commit log, not log processing.

---

## 3. Kafka concepts (the vocabulary)

- **Topic** — a named stream of messages of one type. Producers publish *to* a topic.
- **Producer** — publishes messages to a topic.
- **Broker** — a server that stores published messages.
- **Consumer** — subscribes to one or more topics and **pulls** messages from brokers.
- **Partition** — a topic is split into partitions; each broker holds one or more partitions.
- **Message** — just a payload of bytes. You pick the serialization (e.g. Avro).

### Tiny code feel from the paper

Producer:
```
producer = new Producer(...)
message  = new Message("test message str".getBytes())
set      = new MessageSet(message)
producer.send("topic1", set)
```

Consumer:
```
streams[] = Consumer.createMessageStreams("topic1", 1)
for (message : streams[0]) {
    bytes = message.payload()
    // do something
}
```

- The iterator **never terminates** — if no new messages, it blocks until there are.
- Supports both:
  - **Point-to-point** → multiple consumers share one copy of the topic.
  - **Publish/subscribe** → each consumer group gets its own full copy.

### Architecture sketch (Figure 1)

```
            +------------+    +------------+    +------------+
            |  BROKER 1  |    |  BROKER 2  |    |  BROKER 3  |
            | topic1/p1  |    | topic1/p1  |    | topic1/p1  |
            | topic1/p2  |    | topic1/p2  |    | topic1/p2  |
            | topic2/p1  |    | topic2/p1  |    | topic2/p1  |
            +------------+    +------------+    +------------+
                 ^                  ^                  ^
        producers|publish    consumers|pull
```

---

## 4. Single-partition efficiency (Section 3.1)

### Simple storage layout
- Each partition = one **logical log**.
- Physical log = a series of **segment files** of roughly equal size (e.g. 1 GB).
- Publish = **append** to the last segment file. That's it. No B-trees, no random writes.
- Segment is **flushed to disk** only after N messages or T seconds.
  - A message is **visible to consumers only after flush**.

### No message IDs — just offsets
- A message has **no explicit ID**. It's addressed by its **byte offset in the log**.
- Saves the cost of maintaining a seek-heavy ID→location index.
- Offsets are **increasing but not consecutive**: next_offset = current_offset + message_length.
- "Message ID" and "offset" are used interchangeably.

> **[Modern Kafka — 2025] — answer to your TODO about Control Center showing consecutive offsets**
> The 2011 paper used **byte offsets** (position in the log file) → so they jumped by message size, like 0 → 215 → 1451.
> Kafka changed this very early (around 0.8). Today, offsets are **simple per-message counters**: 0, 1, 2, 3, ...
> That's exactly what you see in Control Center. The byte-offset story above is the *original* design; the **logical offset (sequential int)** is the current one. Everything else (consumer reads from an offset, broker uses an in-memory index) still works the same way — only the numbering scheme changed.

### How a consumer reads
- Consumer always reads a partition **sequentially**.
- Acking offset N means "I've got everything up to N in this partition."
- Under the hood: consumer fires **async pull requests** = `(start_offset, max_bytes)`.
- Broker keeps an **in-memory sorted list** of "first offset in each segment file" → binary-search to find the right segment.

### In-memory index sketch (Figure 2)
```
segment file 1                  segment file N
+-----------------+             +-----------------+
| msg-0000000000  |   append--> | msg-0205070677  |
| msg-0000000215  |             | msg-0205070694  |
| msg-0001451680  |             | msg-0261451680  |
+-----------------+             +-----------------+
        ^                                ^
        |                                |
        +----- reads <----- in-memory index of segment start offsets
```

### Efficient data transfer
- **Producer batches** messages in one `send` call.
- **Consumer pull also returns many messages** at once (hundreds of KB per request).
- **No in-process message cache** — Kafka relies on the **OS page cache**.
  - Benefit 1: no double buffering (page cache only).
  - Benefit 2: cache stays warm across broker process restarts.
  - Benefit 3: tiny GC pressure → fine to implement in a VM language (JVM).
- Sequential reads/writes make **OS read-ahead and write-through caching** very effective.
- Performance stays **linear up to many TB** of data.

### `sendfile` zero-copy
Normal "file → socket" path:
1. Disk → page cache
2. Page cache → app buffer
3. App buffer → kernel socket buffer
4. Socket buffer → NIC
= **4 copies + 2 syscalls**.

Linux `sendfile` collapses steps 2–3 → saves **2 copies + 1 syscall**. Kafka uses it to ship segment-file bytes straight to the consumer socket.

### Stateless broker
- Broker does **not** track what each consumer has consumed. The **consumer tracks its own offset**.
- This is huge — broker stays simple, no per-consumer state.
- But it creates a question: *when can we delete a message?*
  - **Time-based retention SLA** — delete after N days (typical: 7 days).
  - Works because most consumers (online or offline) finish in real-time, hourly, or daily.
  - And because perf doesn't degrade with bigger data sizes, long retention is cheap.

> **[Modern Kafka — 2025]**
> - Consumers still **own** their offset, but they **commit it to Kafka itself** — into an internal topic called **`__consumer_offsets`** (since Kafka 0.9). No more ZooKeeper for offsets.
> - Retention is now configurable as **time** (`retention.ms`), **size** (`retention.bytes`), or **log compaction** (keep only the latest value per key — useful for "current state" topics).
> - **Tiered storage** (Kafka 3.6+, KIP-405) can push old segments to **object storage (S3 etc.)** → effectively infinite retention at low cost.

### Side benefit: consumers can rewind
- Breaks the classic queue contract, but it's a feature, not a bug.
- **Use case 1** — bug in consumer code: fix it, then replay messages.
- **Use case 2** — consumer flushes to a sink only periodically (e.g. a full-text indexer). If it crashes, checkpoint the smallest unflushed offset and re-consume from there.
- Rewinding is **much easier in pull than push**.

---

## 5. Distributed coordination (Section 3.2)

### Producer side
- A producer can publish to:
  - A **randomly chosen** partition, OR
  - A partition picked by a **(key, partitioning function)** — semantically.

### Consumer side: consumer groups
- A **consumer group** = one or more consumers that **jointly** consume the subscribed topics.
- Within a group: each message goes to **exactly one** consumer.
- Different groups: each gets its **own full copy** of the topic.
- No coordination needed *across* groups.

### Smallest unit of parallelism = partition
- **Rule**: at any time, each partition is consumed by **only one consumer in a group**.
- Why? If multiple consumers shared a partition, they'd need locks + shared state to track who read what. Single-owner avoids all that.
- **Implication**: # of partitions ≥ # of consumers in any group → otherwise some consumers sit idle.
  - Fix: **over-partition** the topic from the start.

### Scenarios (same flavor as the docx)

**Scenario A — Balanced (partitions == consumers)**
4 partitions, 4 consumers:
- Partition 0 → Consumer A
- Partition 1 → Consumer B
- Partition 2 → Consumer C
- Partition 3 → Consumer D

**Scenario B — Fewer consumers than partitions**
4 partitions, 2 consumers:
- Partition 0 & 1 → Consumer A
- Partition 2 & 3 → Consumer B

Perfectly fine. A just does twice the work. (This is **1-to-Many**: one consumer can own many partitions, but one partition has exactly one owner.)

**Scenario C — More consumers than partitions**
2 partitions, 3 consumers:
- Partition 0 → Consumer A
- Partition 1 → Consumer B
- Consumer C → **Idle**

Kafka won't hand C any partition because both are already owned. Lesson: **always over-partition**.

### No master node — Zookeeper instead
- No central "master" — masters add failure modes.
- Consumers coordinate **among themselves** via **Zookeeper**.
- Zookeeper is a tiny filesystem-like API: create / set / read / delete / list paths, plus:
  - **Watchers** — get notified when a path changes.
  - **Ephemeral paths** — auto-deleted when the creator disconnects.
  - **Replicated data** — highly reliable.

> **[Modern Kafka — 2025] — ZooKeeper is gone**
> - Modern Kafka uses **KRaft** (Kafka Raft) instead of ZooKeeper. KRaft was GA in **Kafka 3.3 (2022)**, default in **3.5**, and ZooKeeper was **removed entirely in Kafka 4.0 (2025)**.
> - With KRaft, a small subset of brokers act as **controllers** (running Raft consensus) — no external service needed.
> - For consumer coordination, even before KRaft, Kafka 0.9 moved that responsibility into the brokers themselves (see the **GroupCoordinator** note in the rebalance section below).
> - The decentralized "consumers compute the assignment themselves" model below is **historical** — today the broker-side coordinator does it.

### What Kafka stores in Zookeeper
| Registry | What's in it | Lifetime |
|---|---|---|
| **Broker registry** | broker host:port, list of (topic, partition) hosted | ephemeral |
| **Consumer registry** | consumer's group + subscribed topics | ephemeral |
| **Ownership registry** | for each subscribed partition → consumer ID that owns it | ephemeral |
| **Offset registry** | for each subscribed partition → last consumed offset | **persistent** |

- Ephemeral entries vanish if the broker / consumer dies → reflects current liveness.
- The offset registry survives consumer restarts (otherwise you'd lose your place).

### Kafka uses Zookeeper for:
1. Detecting brokers / consumers joining or leaving.
2. Triggering **rebalance** in each consumer when (1) happens.
3. Tracking which consumer owns which partition + the last consumed offset.

> **[Modern Kafka — 2025]**
> Only (1) — *broker* discovery — used to belong to ZooKeeper, and that's now handled by **KRaft controllers**.
> Consumer-side work (2 and 3) moved out of ZooKeeper years ago:
> - Group membership + assignment → handled by the **GroupCoordinator** broker (since 0.9).
> - Offsets → stored in the **`__consumer_offsets`** topic (since 0.9).
> So the "registries in ZooKeeper" table above is purely historical for modern clusters.

### Rebalance algorithm (Algorithm 1)

**Simple explanation (answer to your TODO):**
The trick is that **no one is in charge** — every consumer figures out its share by following the same recipe. Five steps:

1. **Drop everything.** The consumer releases all partitions it currently owns (so nothing is double-owned during the handover).
2. **Look around.** It reads two lists from ZooKeeper: *who's alive in my group* and *what partitions exist for the topics I follow.*
3. **Sort both lists.** Everyone sorts them the same way, so every consumer in the group ends up with an **identical view**.
4. **Find your slot.** "I'm consumer #2 of 4, so I take partitions #2 of 4." Just divide partitions evenly and pick your slice by index.
5. **Claim and start reading.** For each picked partition, write "I own this" in ZooKeeper, look up the last committed offset, and start pulling from there.

Because all consumers see the same input and follow the same recipe, they all arrive at the **same assignment** without talking to each other. No master, no negotiation.

> **[Modern Kafka — 2025]**
> The "everyone computes it themselves from ZooKeeper" model was replaced in **Kafka 0.9** by a **GroupCoordinator** — one broker is elected as coordinator for each consumer group, consumers send it heartbeats, and **it** decides the assignment and ships it to everyone.
> Modern strategies are pluggable: **Range**, **RoundRobin**, **Sticky** (try to keep each consumer's old partitions on rebalance), and **CooperativeSticky** (rebalance *incrementally* — no more "stop the world, drop everything" pause).

When a consumer `Ci` in group `G` rebalances:
```
For each topic T that Ci subscribes to {
    1. Drop Ci's current partitions from the ownership registry.
    2. Read broker registry + consumer registry from Zookeeper.
    3. PT = all partitions of T across all brokers.
    4. CT = all consumers in G subscribed to T.
    5. Sort PT and CT (so all consumers compute the same ordering).
    6. Let j = index of Ci in CT,  N = |PT| / |CT|.
    7. Ci claims partitions PT[j*N ... (j+1)*N - 1].
    8. For each claimed partition p {
         - Write Ci as owner of p in ownership registry.
         - Read last offset Op from offset registry.
         - Start a thread to pull p from offset Op.
       }
}
```

- All consumers see the same broker / consumer registries → they all compute the same partitioning → no central coordinator needed.
- Periodically each consumer **updates its consumed offset** in the offset registry.

### What if two consumers race for the same partition?
- Notifications can arrive at slightly different times.
- A consumer that tries to claim a partition still owned by someone else **releases everything, waits, retries**.
- In practice, settles after a few retries.

### What if the offset registry is empty (brand-new group)?
- Consumers start from the **smallest** or the **largest** offset, depending on config.

---

## 6. Delivery guarantees (Section 3.3)

- Kafka guarantees **at-least-once** delivery.
  - **Exactly-once** would need 2-phase commits — overkill for logs.
- "Most of the time, exactly once" — duplicates happen only when a consumer crashes **without a clean shutdown** before checkpointing.
  - If duplicates matter, add app-level **de-dup using offsets or a message key**.
- **Order**:
  - Within a partition → **strict order guaranteed**.
  - Across partitions → **no order guarantee**.
- **Data integrity**:
  - Each message has a **CRC** in the log.
  - On startup, a recovery process strips messages with bad CRCs (I/O errors).
  - CRC also catches network corruption on the wire.
- **Failure mode (in 2011)**:
  - If a broker dies, its unconsumed messages are **temporarily unavailable**.
  - If its disk is destroyed, those messages are **lost forever**.
  - Replication is listed as **future work** (and was added in later Kafka versions).

> **[Modern Kafka — 2025] — most of the 2011 limitations are fixed**
> - **Exactly-once is real.** Kafka 0.11 (2017) added the **idempotent producer** (`enable.idempotence=true`) so retries can't create duplicates, plus a **transactional API** for atomic "read → process → write" across multiple partitions.
> - **Replication is built-in** (since 0.8). Every partition has a **leader** + **followers**, and the set of up-to-date copies is the **ISR (in-sync replicas)**.
>   - Producer durability is tunable via **`acks`**: `0` (fire-and-forget), `1` (leader wrote it), `all` (every ISR wrote it).
>   - `min.insync.replicas` sets a floor — e.g. require at least 2 copies before accepting writes.
>   - On broker death, a follower in the ISR is promoted to leader → no data loss if `acks=all`.
> - **Order:** unchanged — still in-order within a partition, no guarantee across partitions. (Idempotence preserves order on retries too.)
> - **CRC + recovery:** still the same idea — modern Kafka uses CRC32C per **record batch**.

---

## 7. Kafka at LinkedIn (Section 4)

### Deployment topology (Figure 3)
```
   MAIN DATACENTER                      ANALYSIS DATACENTER
   +-----------+                        +-----------+
   | frontend  | --\                    |  broker   |
   | frontend  | ---> load balancer --> |  (replica |  ---> Hadoop
   | frontend  | --/    |   |          |   cluster)|  ---> Data warehouse
                        v   v          +-----------+
                    +-------+
                    | broker|
                    +-------+
                        |
                  realtime services
```

- **One Kafka cluster per datacenter**, sitting next to the frontends.
- Hardware load balancer spreads publishes across brokers.
- Online consumers run in the same datacenter as the brokers.
- A **separate Kafka cluster** in the analysis datacenter runs **embedded consumers** that pull from the live clusters → MapReduce jobs then load it into Hadoop / DWH.
- **End-to-end latency** ≈ **10 seconds**.
- Volume: hundreds of GB and ~1 billion messages / day (in 2011 terms).

### Auditing pipeline
- Each message carries a **timestamp + server name**.
- Each producer periodically emits a **monitoring event** to a dedicated audit topic — "I produced X messages on topic T in the last window."
- Consumers tally their received counts and compare with the audit events → detect data loss.

### Hadoop loading
- A custom **Kafka input format** for MapReduce reads directly from Kafka.
- Both **raw data and offsets** are written to HDFS only when the job **completes successfully**.
- Because brokers are stateless and offsets are client-side, MR's task-restart machinery just works — no dupes, no losses on retry.

### Serialization: Avro
- Avro chosen for efficiency + **schema evolution**.
- Wire format = `(avro_schema_id, serialized_bytes)`.
- A **lightweight schema registry** maps `schema_id → schema`.
- Consumer looks up the schema **once per ID** (immutable) → decodes the bytes.
- Net effect: enforces a producer ↔ consumer contract.

---

## 8. Experimental results (Section 5)

Setup: 2 machines (8 × 2 GHz, 16 GB RAM, 6 disks RAID-10, 1 Gb link).
Compared against **ActiveMQ 5.4** (JMS) and **RabbitMQ 2.4**.

### Producer test (10 M messages, 200 bytes each)

| System | Throughput (msgs / sec) |
|---|---|
| Kafka, batch = 1 | ~50,000 |
| Kafka, batch = 50 | ~400,000 (almost saturates 1 Gb NIC) |
| ActiveMQ | orders of magnitude lower |
| RabbitMQ | at least 2× lower than Kafka |

**Why Kafka wins on producer:**
1. Producer **doesn't wait for broker ack** → throughput screams. (Trade-off: occasional message can be dropped — fine for logs.)
2. **Storage overhead per message**: Kafka ≈ **9 bytes**, ActiveMQ ≈ **144 bytes** (70 % more space). JMS headers + B-tree metadata are heavy.
3. **Batching** amortizes the RPC overhead — batch of 50 → ~10× throughput.

### Consumer test (10 M messages, ~200 KB / 1000 msgs per pull)

| System | Throughput (msgs / sec) |
|---|---|
| Kafka | ~22,000 |
| ActiveMQ | <¼ of Kafka |
| RabbitMQ | <¼ of Kafka |

**Why Kafka wins on consumer:**
1. Smaller wire format → fewer bytes to ship.
2. ActiveMQ / RabbitMQ track **per-message delivery state** → broker is busy writing state to disk. Kafka broker does **zero disk writes** during consumption.
3. **`sendfile`** cuts copy and syscall overhead.

> The paper's own caveat: the goal isn't to dunk on ActiveMQ / RabbitMQ — those have more features (transactions, per-message acks). The point is to show what a **specialized** system can achieve.

---

## 9. Conclusion + what was coming next

- **Pull model** lets consumers go at their own pace and rewind.
- Focused scope (log processing) → way higher throughput than general-purpose messaging.
- Distributed support is baked in — scale out by adding brokers.

### Future work the paper called out
1. **Built-in replication** across brokers → durability + availability on disk failure.
   - Both **async** (lower producer latency) and **sync** (stronger guarantees) modes — let the app pick.
2. **Stream processing** primitives — windowed counts, joins against another stream, joins against a key-value store.
   - Foundation: partition messages by the **join key** so all related messages land in one consumer.
   - On top of that: a library of windowing / join utilities.

> Historical note: both of these *did* land in later Kafka versions — replication shipped in 0.8, and the stream-processing library became **Kafka Streams**.

---

## 10. One-page mental model

- **Topic** → split into **partitions** → spread across **brokers**.
- **Producer** appends to a partition's log; the log is a sequence of **segment files**; addressing is by **byte offset**.
- **Consumer** pulls from a partition starting at its last offset; **consumer is responsible for its own offset**.
- Inside a **consumer group**, each partition is owned by **exactly one** consumer.
- **Zookeeper** holds liveness + ownership + offsets so consumers can rebalance themselves with no master.
- **OS page cache + `sendfile` + batching + append-only logs** = throughput.
- **Time-based retention (~7 days)** lets brokers stay stateless and lets consumers rewind.
- **At-least-once** delivery, **in-order within a partition**, CRC per message.

> **[Modern Kafka — 2025] — the same model with the 2025 swaps**
> - Offsets are **sequential per-message ints (0, 1, 2…)**, not byte positions.
> - **No ZooKeeper** — cluster metadata lives in **KRaft controllers**; consumer offsets + group state live **in Kafka itself** (`__consumer_offsets`, GroupCoordinator).
> - Partitions are **replicated** (leader + followers + ISR); producer picks durability with `acks`.
> - Retention can be time / size / **compaction**, and old segments can spill to **tiered (S3) storage**.
> - **Exactly-once** is available end-to-end (idempotent producer + transactions).
> - Everything else (pull model, page-cache reliance, sendfile, in-order per partition, one owner per partition per group) is unchanged.

---

## 11. What's new since 2011 — cheatsheet

A compact list of everything that has changed since the white paper, in order. If you're coming from the 2011 paper, this is the diff.

| Version (year) | What landed | Why it matters |
|---|---|---|
| **0.8 (2013)** | **Replication** (leader/follower, ISR), new offset = sequential int | No data loss on broker failure; paper's biggest "future work" item done |
| **0.8.1 (2014)** | **Log compaction** | Keep only the latest value per key → use a topic as a "current state" store |
| **0.9 (2015)** | **`__consumer_offsets` topic**, **GroupCoordinator**, **Kafka Connect**, security (SASL/SSL/ACLs) | Offsets + group mgmt out of ZooKeeper; multi-tenant clusters became safe |
| **0.10 (2016)** | **Kafka Streams**, message **timestamps**, rack awareness | DSL for stream processing without an external framework |
| **0.11 (2017)** | **Idempotent producer**, **transactions** → **exactly-once semantics**, message **headers**, new compact record-batch format | The single biggest leap in correctness |
| **1.x – 2.x (2017–2019)** | JBOD improvements, better admin API, **incremental fetch**, perf tuning | Operational maturity |
| **2.4 (2019)** | **Cooperative / Incremental rebalancing** | Rebalances no longer "stop the world" — consumers keep their unaffected partitions |
| **2.8 (2021)** | **KRaft preview** (Kafka without ZooKeeper) | First glimpse of ZK-free Kafka |
| **3.3 (2022)** | **KRaft GA** for new clusters | ZooKeeper officially optional |
| **3.5 (2023)** | KRaft is the default for new clusters | Most new deployments are ZK-free |
| **3.6 (2023)** | **Tiered storage (KIP-405)** | Old segments offload to S3-class storage → cheap "forever" retention |
| **3.7 (2024)** | **KIP-848** ("next-gen consumer protocol") preview | Rebalance logic moved fully to the broker, faster + simpler client |
| **4.0 (2025)** | **ZooKeeper removed entirely**, KIP-848 default, JDK 17+ baseline | Modern Kafka = KRaft-only |

### Things from the paper that are now wrong (quick list)
- "Offsets are byte offsets, not consecutive." → Now sequential ints.
- "Broker doesn't track consumer offsets." → Still consumer-driven, but **stored in Kafka** (`__consumer_offsets`), not on the client.
- "ZooKeeper holds broker / consumer / ownership / offset registries." → All four are gone from ZooKeeper. Broker metadata → KRaft. Consumer side → GroupCoordinator + `__consumer_offsets`.
- "Consumers compute the rebalance themselves." → Broker-side GroupCoordinator decides; clients just receive the assignment.
- "Kafka only guarantees at-least-once." → Exactly-once is supported.
- "If a broker's disk dies, messages are lost." → Replication + ISR + `acks=all` prevent this.
- "Producer doesn't wait for acks." → `acks` is configurable (0 / 1 / all); `all` is the modern default for durable pipelines.
- "Retention is time-based, ~7 days." → Time **or** size **or** compaction, and tiered storage extends it indefinitely.
- "Avro + a lightweight schema registry." → **Confluent Schema Registry** is the de-facto choice and supports **Avro, JSON Schema, and Protobuf**.

### New ecosystem pieces the paper couldn't mention
- **Kafka Connect** — declarative source/sink connectors (DBs, S3, Elasticsearch…). No more bespoke MapReduce loaders.
- **Kafka Streams** — JVM library for stateful stream processing on top of Kafka (windows, joins, state stores).
- **ksqlDB** — SQL over Kafka Streams.
- **MirrorMaker 2** — cross-cluster replication built on Kafka Connect (the modern replacement for the LinkedIn "embedded consumer" pattern in Section 7).
- **Cruise Control** — automated partition rebalancing across brokers.

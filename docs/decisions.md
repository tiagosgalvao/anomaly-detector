# Design decisions

A record of the choices made building this service, and what each one costs.
The brief leaves most of these open, so the reasoning matters more than the
outcome.

## At a glance

| # | Decision | In one line |
| --- | --- | --- |
| [1](#1-three-services-not-the-single-container-simplification) | Three services, not the permitted single container | The queue mechanics are most of the exercise, so the harder path is the one worth taking. |
| [2](#2-kafka-as-the-messaging-system) | Kafka, in KRaft mode | Chosen for **replay**, not scale: tuning a detector needs the same data re-run under new parameters. |
| [3](#3-kafka-streams-is-not-used) | No Kafka Streams | Its windowing is time-based; this window is count-based, so the ring buffer gets written either way. |
| [4](#4-one-series-one-partition--with-per-series-windows-regardless) | One series, one partition | Correct for one order-sensitive series — but windows are keyed by series anyway. |
| [5](#5-json-on-the-wire-not-a-bare-number) | JSON payload, not a bare number | A bare double forfeits the partition key, the timestamp and any identifier. |
| [6](#6-no-shared-dto-artifact--the-consumer-is-a-tolerant-reader) | No shared DTO — tolerant reader | Keeps deployments independent, and makes the ground-truth flag structurally unreachable from the detector. |
| [7](#7-sample-standard-deviation-not-population) | Sample standard deviation (n − 1) | The window is a sample of an ongoing process, not a population. |
| [8](#8-mean-and-sigma-recomputed-per-point-on-rather-than-maintained-in-o1) | O(N) recompute, not O(1) running sums | Clarity over cleverness; running sums drift and can catastrophically cancel. |
| [9](#9-each-point-is-scored-against-the-window-before-it-is-inserted) | Score before inserting the point | A point inside its own baseline contaminates it and caps the Z-score at ≈6.93. |
| [10](#10-detected-anomalies-are-excluded-from-the-window) | Anomalies excluded from the window | Prevents sigma inflation — at the cost of never adapting to a genuine level shift. |
| [11](#11-a-threshold-of-z--3-has-a-known-false-positive-rate) | Z > 3 flags ~0.27% by construction | That is the definition of the threshold, not a defect. Measured at 0.23%. |
| [12](#12-non-finite-values-are-rejected-at-the-boundary) | Non-finite values rejected at the boundary | One `NaN` in the ring buffer poisons every later Z-score. |
| [13](#13-at-least-once-delivery) | At-least-once delivery | Redelivery is preferable to loss; the cost is a point that can enter the window twice. |
| [14](#14-two-independent-projects-rather-than-a-multi-module-build) | Two independent Gradle projects | A repository boundary is not a build boundary; each service owns its Docker context. |
| [15](#15-gradle-over-maven) | Gradle over Maven | Familiarity, with Maven's cleaner dependency-caching idiom acknowledged. |
| [16](#16-kotlin-dsl-for-the-build-scripts) | Kotlin DSL for build scripts | Statically typed build files; Gradle's default for new builds since 8.2. |
| [17](#17-a-pinned-toolchain-image-for-container-builds) | Pinned Gradle image, not the wrapper | The image must not depend on a jar an email filter can strip. |
| [18](#18-two-broker-listeners-one-internal-and-one-for-the-host) | Two broker listeners | Clients connect to what the broker *advertises*, so the host needs its own listener. |
| [19](#19-the-broker-is-gated-by-a-healthcheck-and-its-data-outlives-the-container) | Healthcheck gate, data on a volume | Without it both services crash-loop on a cold start; the volume is what makes replay real. |
| [20](#20-layered-images-built-by-a-pinned-gradle-run-as-a-non-root-user) | Layered images, non-root runtime | Dependencies cached separately from application code; no build tooling in the runtime image. |
| [21](#21-the-services-are-kept-alive-explicitly) | Liveness stated, not inherited | Headless Boot apps exit 0; under `restart: unless-stopped` that is a restart loop. |
| [22](#22-the-producers-wire-format-is-configured-not-hand-rolled) | Configured serializers, no type headers | Jackson 3 and renamed spring-kafka classes; the type header would couple the services. |
| [23](#23-configuration-is-validated-when-the-context-starts-not-when-a-value-is-used) | Configuration validated at startup | A bad value fails the container immediately instead of emitting `NaN` much later. |
| [24](#24-the-producer-publishes-with-acksall) | `acks=all` on the producer | The only setting that keeps the idempotent producer; `acks=1` disables it *silently*. |
| [25](#25-the-producer-sends-a-primitive-double-the-consumer-reads-a-boxed-double) | `double` out, `Double` in | Only the parsing side can encounter absence, and a primitive would fabricate `0.0`. |
| [26](#26-the-consumer-states-its-target-type-and-refuses-type-headers) | Consumer states its own target type | It never takes deserialisation instructions from the wire. |

---

## Architecture

### 1. Three services, not the single-container simplification

The brief permits collapsing everything into one container that generates its
own data, with no queue and no separate producer. That option is declined: the
three-service layout is the primary target, and the queue mechanics are most of
what makes the exercise interesting. The cost is a slower cold start and more
moving parts to get right for a first run.

### 2. Kafka as the messaging system

The brief names RabbitMQ, Redis Pub/Sub and SNS/SQS as examples, not as a closed
list, and asks for something "lightweight" — which is read as operational
burden. Kafka in KRaft mode is a single container with no ZooKeeper, so it
meets that.

The reason for choosing it is **replay**, not scale. A detector's window size
and threshold are not knowable in advance; they are arrived at by trying values
and comparing results. Because Kafka retains the log, resetting a consumer
group's offsets re-runs the detector over byte-identical data under new
parameters. A queue destroys on consume, so re-testing means regenerating the
stream — and being random, it is not the same stream. Retention gives the
related property that an alert can be traced back to the exact record that
produced it, which is a routine operational question for anything that fires
alarms.

Both properties are asserted in `ReplayIntegrationTest`: one test resets the
consumer group's offset and checks the same records are delivered a second time,
the other stops the consumer, publishes while it is down, and checks nothing is
missed. The argument for the broker is therefore executable, not just prose.

**Cost, stated plainly:** Kafka is the heaviest of the four options. It needs a
capped heap and several seconds to become ready, which forces a healthcheck and
`depends_on: condition: service_healthy` for a clean first run. At the scope
actually submitted — one series, one partition, one consumer — RabbitMQ would
serve the functionality equally well. The Kafka-specific value here is replay
and retention, not throughput.

The official `apache/kafka` image is used rather than `confluentinc/cp-kafka`
(Confluent Community License) or Bitnami's (moved to paid Secure Images, with
free tags relegated to `bitnamilegacy`).

### 3. Kafka Streams is not used

Streams cannot supply this window. Its windowing DSL is entirely time-based —
tumbling, hopping, sliding, session — while the brief's window is count-based:
the most recent N points. There is no "last N records" construct in the DSL, so
implementing it means dropping to the Processor API with a `KeyValueStore`
wrapping the same ring buffer. The ring buffer gets written either way.

What Streams would add is durability of that window state across rebalances,
via a changelog topic, plus exactly-once semantics. Neither is required here,
and both cost something: a single-broker cluster needs its internal topic
replication factors overridden or startup fails, and the bundled RocksDB will
not load against musl, ruling out an Alpine runtime.

`RollingWindow` and `ZScoreDetector` are therefore kept free of any Kafka
import, so the migration path stays open. See **Up Next** in the README.

### 4. One series, one partition — with per-series windows regardless

The brief describes monitoring "a simple metric" (singular), and the mandated
output line carries no field for a series identifier. Interleaving several
sensors would make the log ambiguous and force a deviation from one of the few
things the brief specifies verbatim. With a single series, extra partitions
would sit permanently empty.

So: one series, one partition. For a single order-sensitive series that is the
correct configuration, not a limitation — additional partitions would buy
parallelism that cannot be used while breaking the ordering the rolling window
depends on.

The consumer nevertheless keys its windows by series (`Map<String,
RollingWindow>`). This is about three lines more than a single field and is not
speculative: anomaly detection is inherently per-series, and pooling readings
from different sources into one window is statistically wrong. Messages are
keyed by `seriesId` for the same reason. Adding a second series becomes a
configuration change rather than a rewrite.

`RollingWindow` itself is deliberately not thread-safe: one instance belongs to
one series, and Kafka guarantees a partition is consumed by a single thread, so
no instance is ever touched concurrently. Adding synchronisation would defend
against a case that cannot occur.

---

## Message contract

### 5. JSON on the wire, not a bare number

A bare floating-point value would be simpler, but it forfeits the partition key,
the producer timestamp (and with it end-to-end latency and the event-time versus
processing-time distinction), and any identifier for deduplication. The payload
carries `id`, `seriesId`, `timestamp`, `value` and `injectedAnomaly`.

### 6. No shared DTO artifact — the consumer is a tolerant reader

Producer and consumer are independent projects with no shared module and no
published contract jar. A shared jar would couple their deployments into
lockstep releases: change the record, rebuild both.

Instead the consumer defines its own minimal record and ignores unknown fields,
so the producer can add fields without breaking it. This also makes a design
constraint structural rather than a matter of discipline: `injectedAnomaly` is
ground truth used only for development and verification, and the detector must
never read it. With a shared DTO that field would sit in the consumer's model,
one autocomplete away from silently invalidating every result. With separate
models it is unreachable.

The overlap is roughly a dozen lines, and they are not two copies of one thing —
they are the producer's output contract and the consumer's input projection,
which legitimately differ. The scaled-up answer is schema-first code generation
against a registry, not a shared jar; see **Up Next**.

---

## Statistical model

### 7. Sample standard deviation, not population

The window is a sample of an ongoing process, not a complete population, so the
`n − 1` denominator is used. At n = 50 the difference is around 1%.

### 8. Mean and sigma recomputed per point, O(N), rather than maintained in O(1)

Running sums of values and squares would make each update constant-time, but
they accumulate floating-point drift and are vulnerable to catastrophic
cancellation over long runs. At N = 50 and ten points per second, recomputing is
roughly 500 floating-point operations per second — free. This is a deliberate
choice of clarity over cleverness, and the point at which it would be revisited
is throughput high enough for the O(N) pass to matter.

### 9. Each point is scored against the window *before* it is inserted

If the new point is already inside the window it contaminates its own baseline,
inflating sigma and pulling the mean toward itself. It also caps the result: for
a point within an n-sample window, the Z-score is mathematically bounded by
`(n − 1) / √n`, which is about 6.93 at n = 50 — a ceiling that cannot be
exceeded no matter how extreme the value. Scoring against the prior window
removes both problems.

### 10. Detected anomalies are excluded from the window

Admitting them inflates sigma and masks the next genuine anomaly. **The cost is
real and worth stating:** a legitimate level shift — the metric moving to a new
normal — is then rejected indefinitely, because the window never adapts to it.
Mitigation belongs in a change-point detector or a rule admitting values after
k consecutive rejections; see **Up Next**.

### 11. A threshold of Z > 3 has a known false-positive rate

On genuinely normal data, roughly 0.27% of points exceed three standard
deviations by construction — at ten points per second, a false alarm every few
minutes. This is the definition of the threshold, not a defect. Because the mean
and sigma are *estimated* from a finite window rather than known, the real tail
is somewhat fatter than the normal distribution implies, so the observed rate
runs slightly higher.

### 12. Non-finite values are rejected at the boundary

A single `NaN` or `Infinity` entering the ring buffer makes the mean and sigma
`NaN`, and every subsequent Z-score with them — the detector dies silently and
does not recover cleanly even as the value ages out. Values are checked with
`Double.isFinite` on arrival and routed to the dead-letter topic, and validated
producer-side before publication.

---

## Delivery and build

### 13. At-least-once delivery

Auto-commit is disabled and offsets are committed after processing, so a crash
between the two causes redelivery rather than loss. The consequence is owned:
the detector is not idempotent, and a redelivered point enters the window twice,
very slightly skewing it. Given the alternative is losing data points entirely,
that is the right trade for this workload.

### 14. Two independent projects rather than a multi-module build

Each service is its own self-contained Gradle project inside one repository.
Each therefore has its own Docker build context, so no sibling source leaks into
an image and each builds standalone. A Git repository boundary is not a build
boundary. The cost is that Java and Spring Boot versions must be kept in step by
hand, which a multi-module build or version catalog would otherwise enforce.

### 15. Gradle over Maven

Gradle, on familiarity. Maven would be equally defensible here, and it has one
concrete advantage worth acknowledging: its `dependency:go-offline` idiom gives
cleaner Docker layer caching than any Gradle equivalent, because dependency
resolution can be made its own cacheable build step ahead of copying source.

### 16. Kotlin DSL for the build scripts

Gradle build files are programs rather than declarative data, so they need a
language. Gradle offers two: the original Groovy DSL (`build.gradle`) and the
Kotlin DSL (`build.gradle.kts`). Same Gradle, same plugins, same capabilities —
only the scripting language differs.

The Kotlin DSL is used here. Build scripts are statically typed, so a mistyped
property or task name is a compile error before anything executes rather than a
failure partway through a build, and an IDE can offer real completion and
navigate into plugin sources instead of guessing at dynamic Groovy. It has also
been Gradle's default for new builds since 8.2.

**Cost:** most Gradle material online is written in Groovy, so answers frequently
need translating, and the first configuration is slower because the script itself
must compile. Neither weighs heavily here. Gradle itself is familiar ground — it
is the Kotlin scripting flavour that is new — so translating a Groovy snippet is
mechanical rather than research. And each project is roughly twenty-five lines
with four dependencies and no custom tasks, where the syntactic difference
amounts to parentheses and double quotes.

### 17. A pinned toolchain image for container builds

Container builds use a pinned Gradle image and invoke Gradle directly rather
than going through the wrapper, so the build does not depend on bootstrapping a
downloader it does not need. The wrapper stays in the tree for local development,
and is committed deliberately — the standard Java `.gitignore` template excludes
`*.jar`, which would silently omit `gradle-wrapper.jar` and leave `./gradlew`
broken for anyone cloning the repository.

### 18. Two broker listeners, one internal and one for the host

The broker advertises `kafka:9092` on an internal listener and `localhost:29092`
on a published one. A Kafka client does not talk to the address it bootstrapped
against; it fetches cluster metadata and then connects to whatever the broker
*advertises*. A single listener advertising `kafka:9092` would therefore be
unreachable from the host even with the port published, because `kafka` does not
resolve outside the Compose network. The internal listener keeps the canonical
port so containers need no special configuration, and the `local` profile points
at `localhost:29092` for running a service from the IDE against the containerised
broker.

### 19. The broker is gated by a healthcheck, and its data outlives the container

Kafka needs several seconds before it accepts connections, so both services will
crash-loop on a cold start without `depends_on: condition: service_healthy`. The
healthcheck asks the broker for its API versions over the internal listener,
which tests the thing that actually matters — that the listener is accepting
clients — rather than merely that a process exists. Measured cold start to
healthy on this machine is about seven seconds, with `start_period` absorbing the
window before that.

Log directories point at a named volume rather than the image's default under
`/tmp`, so a restarted container keeps its data. This is what makes the replay
argument in decision 2 real: `docker compose down` followed by `up` preserves the
topic, its records and the committed offsets. Topic auto-creation is disabled, so
a typo in a topic name fails loudly instead of silently creating an empty topic;
the topic is created deliberately, by a `NewTopic` bean in the producer.

### 20. Layered images built by a pinned Gradle, run as a non-root user

Both services use the same three-stage Dockerfile. A `gradle:9-jdk25` stage
compiles and packages; a second stage runs Spring Boot's `jarmode=tools`
extractor to split the fat jar into its four layers; a final
`eclipse-temurin:25-jre-alpine` stage copies those layers in slowest-changing
order and runs as an unprivileged user. The runtime image carries a JRE rather
than a JDK, and no build tooling at all — about 366 MB each.

The build stage copies `settings.gradle.kts` and `build.gradle.kts` and resolves
the runtime classpath *before* copying `src`, so a source-only change reuses the
cached dependency layer. This narrows, without closing, the Maven gap noted in
decision 15: measured here, a rebuild after a source edit takes 8 seconds
against 25 for a cold one. A BuildKit cache mount would do better still, but
behaves differently when BuildKit is disabled, and a first run that works
everywhere matters more than a faster second one.

The wrapper is excluded from the build context along with `build/` and `.gradle/`
(see decision 17). Excluding it is what makes the image's independence from it
verifiable rather than merely intended.

The heap is sized with `-XX:MaxRAMPercentage` on the command line rather than
through `JAVA_TOOL_OPTIONS`, because that variable makes the JVM print a
"Picked up" line to stderr — harmless anywhere else, but the consumer's output
format is fixed by the brief and the log should carry nothing that is not meant
to be there.

### 21. The services are kept alive explicitly

Neither service serves HTTP, so nothing in a bare Spring Boot application holds
the JVM open: both start, find no non-daemon thread to wait on, and exit 0.
Under `restart: unless-stopped` that becomes a restart loop — the exact failure
the brief's "must execute cleanly out of the box" is about, arriving not from a
bug but from a healthy application having nothing to do yet.

`spring.main.keep-alive` is set instead of relying on a side effect. Once the
listener container and the scheduler exist, each would hold the JVM open on its
own, so the property becomes redundant — but it states the intent directly
rather than leaving liveness as something that happens to fall out of an
unrelated component.

### 22. The producer's wire format is configured, not hand-rolled

Values are serialised with spring-kafka's `JacksonJsonSerializer` and keys with the
plain `StringSerializer`. Two details are worth knowing, because both differ from
what most published examples show: Boot 4 ships **Jackson 3** (`tools.jackson`,
pulled in explicitly — the Kafka starter brings no JSON library of its own), and
spring-kafka 4 renamed the serializers, deprecating the `JsonSerializer` that Boot
3 material refers to.

`spring.json.add.type.headers` is **disabled**. Left on, every record carries a
`__TypeId__` header naming `com.anomaly.producer.DataPoint`, inviting the consumer
to deserialise into a class it does not have and coupling the two services through
a header. Turning it off is what makes decision 6's tolerant reader real rather
than aspirational, so a test asserts the header is absent.

The producer also sets `acks=all`; that one has enough behind it to be its own
entry — see decision 24.

### 23. Configuration is validated when the context starts, not when a value is used

`ProducerProperties` is a record whose compact constructor rejects a non-finite
mean, a non-positive standard deviation, a probability outside [0, 1] and an
inverted sigma range. A bad value therefore fails the container at startup with a
message naming the property, rather than silently producing `NaN` points that
poison the consumer's window much later and much less obviously. This is done with
a plain constructor rather than `spring-boot-starter-validation`, which would add
a dependency for four checks.

### 24. The producer publishes with `acks=all`

`acks` controls how much durability the broker must demonstrate before a send is
reported successful:

| Setting | The broker replies when | Loses data if |
| --- | --- | --- |
| `0` | never — the client does not wait | anything at all goes wrong; the send is not even confirmed to have arrived |
| `1` | the partition leader has written the record | the leader fails before a follower replicates it |
| `all` | every in-sync replica has written it | every in-sync replica fails |

**On this stack the three are nearly indistinguishable.** With one broker and a
replication factor of 1, the leader *is* the only replica, so `all` and `1`
require the same single write. Choosing `all` on those grounds alone would be
cargo-culting, and worth saying so plainly.

The reason it is set is a second, less obvious effect. Since Kafka 3.0
`enable.idempotence` defaults to `true`, and idempotence *requires* `acks=all`.
When the two conflict the client does not complain — it resolves the conflict by
turning idempotence off. Probing `ProducerConfig` directly with the 4.2.1 client
shows exactly that:

```
acks=all                          -> idempotence=true
acks=1                            -> idempotence=false      <- silently
acks=0                            -> idempotence=false      <- silently
acks=1 + enable.idempotence=true  -> ConfigException: Must set acks to all in
                                     order to use the idempotent producer.
```

So `acks=1` is not the small durability relaxation it appears to be. It quietly
drops the producer's exactly-once-per-partition write guarantee, and the only
visible trace is the absence of `Instantiated an idempotent producer` in the
startup log — which this service does log.

**Why idempotence matters here.** `retries` defaults to `Integer.MAX_VALUE`, so a
send whose acknowledgement is lost in flight *will* be retried. Without
idempotence that retry appends a second copy of the same point. The consumer's
window is order- and count-sensitive: a duplicated value shifts the mean, inflates
sigma, and biases the next Z-score. Duplicates are not an abstract concern for
this workload — they are a direct corruption of the statistic. Idempotence gives
each record a sequence number the broker uses to discard exactly this kind of
retry.

Note that this is a *producer-side* guarantee only, and does not contradict the
at-least-once consumer contract in decision 13: the broker will not store a
duplicate the producer retried, but a consumer that crashes after processing and
before committing will still reprocess a record.

**Cost.** Latency, once there is more than one replica — the leader waits for
followers rather than replying immediately. At ten small messages per second that
is irrelevant, and it is the correct default to carry into a real cluster, where
`acks=all` should be paired with `min.insync.replicas=2` so a lone surviving
replica rejects writes rather than silently accepting them.

### 25. The producer sends a primitive `double`, the consumer reads a boxed `Double`

The asymmetry is deliberate, and boxing both sides "for consistency" was considered
and declined. The two records are not two copies of one type — they are an output
contract and an input projection (decision 6), and they differ because their jobs
differ.

The producer **constructs** its value. It comes from `nextGaussian()` arithmetic over
configuration validated at startup (decision 23), and the publisher checks
`Double.isFinite` before sending. Absence is not reachable, so a boxed type would add
a null case that can never occur — and `Double.isFinite(dataPoint.value())` would
auto-unbox, creating an NPE path where none exists today.

The consumer **parses** its value, and parsing is exactly where absence appears. A
record binds through its canonical constructor, so a payload with no `value` key
supplies the type default — and for a primitive that is `0.0`, indistinguishable from
a genuine reading of zero. Against a window centred on 50 with sigma 5, a fabricated
`0.0` scores **Z = 10** and prints `ANOMALY DETECTED!`. Schema drift would be reported
as a *detection*: silent, and shaped exactly like success. Boxing is the only way to
make "absent" expressible, and tests pin the behaviour rather than trusting it.

Worth knowing, because Jackson 2 material misleads here: of the three relevant
defaults in Jackson 3, `FAIL_ON_UNKNOWN_PROPERTIES` flipped to `false` (tolerant
reading is now the default) and `FAIL_ON_NULL_FOR_PRIMITIVES` flipped to `true`, but
`FAIL_ON_MISSING_CREATOR_PROPERTIES` remains `false` — which is why the absent-field
case survives and needed handling.

### 26. The consumer states its target type and refuses type headers

`spring.json.value.default.type` names the consumer's own record, and
`spring.json.use.type.headers` is disabled. The first is required because the producer
sends no type header (decision 22); the second means that even if some future producer
did send one, it would be ignored rather than obeyed — `JacksonJsonDeserializer`
honours such headers by default, and would try to load a class belonging to another
service.

Together with decision 22 this closes the loop from both ends: the producer does not
put a class name on the wire, and the consumer would not act on one if it found it.
Deserialisation is the consumer's decision alone.

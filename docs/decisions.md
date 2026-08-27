# Design decisions

A record of the choices made building this service, and what each one costs.
The brief leaves most of these open, so the reasoning matters more than the
outcome.

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

**Cost, stated plainly:** Kafka is the heaviest of the four options. It needs a
capped heap and 10–20 seconds to become ready, which forces a healthcheck and
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

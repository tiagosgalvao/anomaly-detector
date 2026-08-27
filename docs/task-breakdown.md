# Tasks

Working checklist. One task is intended to be one commit, so the history reads
as the order the system was actually built in.

Tasks 1–15 make a complete submission. 16 and 17 are optional.

## Phase 1 — Structure and infrastructure

- [X] **1. Repository structure.** `.gitignore` (including the
      `!gradle/wrapper/gradle-wrapper.jar` exception, since the Java template's
      `*.jar` rule would otherwise break `./gradlew` for anyone cloning), `docs/`
      skeleton.
      → `1. initial repository structure + decisions + task-breakdown files`
- [X] **2. Producer skeleton.** Spring Boot project, Gradle Kotlin DSL, Java 25.
      Boots and logs one line.
      → `2. producer spring boot project skeleton`
- [ ] **3. Consumer skeleton.** Same shape.
      → `3. consumer spring boot project skeleton`
- [ ] **4. Kafka service.** `docker-compose.yml` with Kafka only — KRaft mode,
      capped heap, healthcheck. Prove it reaches healthy and create the topic by
      hand to confirm.
      → `4. kafka service in kraft mode + healthcheck`
- [ ] **5. Containerise and orchestrate.** Dockerfiles for both services,
      `.dockerignore`s, compose wiring all three with
      `depends_on: condition: service_healthy`, root `Makefile`.
      → `5. dockerfiles + compose orchestrating all three services`

> **Gate:** `docker compose up --build` from clean must bring up all three
> services with no crash loops before Phase 2 starts. This is the requirement
> most likely to bite late, so it gets proven first.

## Phase 2 — Messaging

- [ ] **6. Producer publishes.** `DataPoint` record, gaussian generator with a
      seeded `Random`, scheduled publisher, topic created via a `NewTopic` bean,
      messages keyed by `seriesId`.
      → `6. producer publishing generated data points to kafka`
- [ ] **7. Consumer subscribes.** Tolerant-reader record, manual acknowledgement,
      auto-commit disabled. Logs raw payloads for now.
      → `7. consumer subscribing to the metrics topic`

## Phase 3 — Detection core

Unhurried. This is the part the technical interview will actually probe.

- [ ] **8. `RollingWindow` + tests.** Fixed-capacity ring buffer, mean and
      sample standard deviation recomputed per call.
      → `8. rolling window with sample statistics + tests`
- [ ] **9. `ZScoreDetector` + tests.** Score against the window *before*
      inserting the point, warm-up handling, near-zero sigma guard, detected
      anomalies excluded from the window.
      → `9. z-score anomaly detector + tests`
- [ ] **10. Wire up output.** Detector into the listener, per-series windows,
      and the exact output format via a dedicated logger and `Locale.ROOT`.
      → `10. detector wired into consumer + spec-compliant output`

## Phase 4 — Robustness

- [ ] **11. Invalid input handling.** `Double.isFinite` guard at the consumer
      boundary, malformed payload handling, dead-letter topic. With tests.
      → `11. invalid payload handling + dead letter topic`
- [ ] **12. Graceful shutdown.** Clean `SIGTERM` handling so `docker compose
      down` drains rather than severs.
      → `12. graceful shutdown handling`

## Phase 5 — Documentation

- [ ] **13. Decision log, final pass.** `docs/decisions.md` is created in task 1
      and kept current as decisions are made; this is the closing review, not a
      reconstruction at the end.
      → `13. final pass on the decision log`
- [ ] **14. README.** What it is, how to run it, sample output, architecture
      diagram, configuration table, the statistical model, the replay
      demonstration, and the **Up Next** section.
      → `14. project readme + up next section`

## Phase 6 — CI and optional extras

- [ ] **15. Continuous integration.** GitHub Actions building and testing both
      projects on push.
      → `15. github actions ci for both services`
- [ ] **16. _Optional._** Testcontainers integration test.
      → `16. kafka integration test with testcontainers`
- [ ] **17. _Optional, cut first._** Precision/recall reporting against the
      producer's ground-truth flag.
      → `17. detection precision and recall reporting`

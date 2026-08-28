# Gradle 9.7.1 here matches the version the wrapper pins, so a container build
# and a local ./gradlew build resolve the same dependencies. The wrapper itself
# is deliberately not used and not copied in: the image must not depend on a
# binary that mail filters strip from a zipped submission.
FROM gradle:9-jdk25 AS build
WORKDIR /build

COPY settings.gradle.kts build.gradle.kts ./
RUN gradle --no-daemon --quiet dependencies --configuration runtimeClasspath

COPY src ./src
RUN gradle --no-daemon bootJar

FROM eclipse-temurin:25-jre-alpine AS extract
WORKDIR /extract
COPY --from=build /build/build/libs/*.jar application.jar
RUN java -Djarmode=tools -jar application.jar extract --layers --launcher --destination layers

FROM eclipse-temurin:25-jre-alpine
RUN addgroup -S app && adduser -S -G app app
WORKDIR /app

# Copied slowest-changing first: dependencies rebuild only when build.gradle.kts
# does, while application/ changes on every source edit.
COPY --from=extract --chown=app:app /extract/layers/dependencies/ ./
COPY --from=extract --chown=app:app /extract/layers/spring-boot-loader/ ./
COPY --from=extract --chown=app:app /extract/layers/snapshot-dependencies/ ./
COPY --from=extract --chown=app:app /extract/layers/application/ ./

USER app

# Set on the command line rather than via JAVA_TOOL_OPTIONS, which would print a
# "Picked up" line to stderr and pollute the consumer's mandated output format.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "org.springframework.boot.loader.launch.JarLauncher"]

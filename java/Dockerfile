FROM gradle:8.12-jdk21 AS builder
WORKDIR /build
COPY settings.gradle.kts build.gradle.kts ./
COPY java/ java/
ARG GITHUB_TOKEN=""
ARG GITHUB_ACTOR=""
ENV GITHUB_TOKEN=${GITHUB_TOKEN}
ENV GITHUB_ACTOR=${GITHUB_ACTOR}
RUN gradle :java:shadowJar --no-daemon

FROM eclipse-temurin:21-jre
COPY --from=builder /build/java/build/libs/*-all.jar /app/adapter.jar
EXPOSE 9090
CMD ["java", "-jar", "/app/adapter.jar"]

# syntax=docker/dockerfile:1
#
# Builds the JCache server from source. Run from the repository root:
#   docker build -f distribution/Dockerfile -t himanshmunjal/jcache .

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q package -pl cache-server -am -DskipTests \
 && cp cache-server/target/cache-server-*.jar /server.jar

FROM eclipse-temurin:21-jre
ARG VERSION=dev
LABEL org.opencontainers.image.title="JCache" \
      org.opencontainers.image.description="In-memory cache server with LRU, LFU and ARC eviction" \
      org.opencontainers.image.source="https://github.com/himanshmunjal/JCache" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"

RUN groupadd --system jcache \
 && useradd --system --gid jcache --no-create-home jcache \
 && mkdir /data \
 && chown jcache:jcache /data

WORKDIR /app
COPY --from=build /server.jar /app/jcache-server.jar

ENV JCACHE_PORT=6379 \
    JCACHE_SNAPSHOT_PATH=/data \
    JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseG1GC"

USER jcache
VOLUME /data
EXPOSE 6379

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/${JCACHE_PORT} && printf "PING\r\n" >&3 && read -r -t 2 reply <&3 && [[ $reply == +PONG* ]]'

# A shell entrypoint so JAVA_OPTS is expanded; exec keeps java as PID 1 so it receives SIGTERM.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/jcache-server.jar \"$@\"", "jcache-server"]

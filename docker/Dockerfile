FROM azul/zulu-openjdk-debian:21.0.1-21.30-jre-headless

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY build/libs/singer-target-clickhouse.jar singer-target-clickhouse.jar

COPY docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

LABEL org.opencontainers.image.source=https://github.com/biron-bi/singer-target-clickhouse

ENTRYPOINT ["/app/entrypoint.sh"]

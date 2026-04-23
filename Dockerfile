FROM eclipse-temurin:21-jre-noble

COPY build/libs/singer-target-clickhouse.jar /opt/target-clickhouse/target-clickhouse.jar

LABEL org.opencontainers.image.source=https://github.com/biron-bi/singer-target-clickhouse

ENTRYPOINT ["java", "-jar", "/opt/target-clickhouse/target-clickhouse.jar"]

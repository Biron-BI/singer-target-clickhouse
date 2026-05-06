#!/bin/sh
exec java $JAVA_OPTS -jar /app/singer-target-clickhouse.jar "$@"

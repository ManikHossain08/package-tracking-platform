# ptp-jobs: the Spark jobs (status processor, replay, silver/gold/dq) on the official Spark image (ADR-0009).
# Build with `make image` (copies the assembly jar into ./build first).
FROM apache/spark:4.1.3-scala2.13-java17-ubuntu@sha256:7aab81b306013f40152e720d20e149a0c1f163f27fc17e4e5220bf860bf82aa2

COPY --chown=spark:spark build/ptp-jobs.jar /opt/ptp/ptp-jobs.jar
COPY --chown=spark:spark log4j2.properties /opt/ptp/log4j2.properties
COPY --chown=spark:spark run.sh /opt/ptp/run.sh

USER spark
WORKDIR /opt/ptp
ENTRYPOINT ["/opt/ptp/run.sh"]

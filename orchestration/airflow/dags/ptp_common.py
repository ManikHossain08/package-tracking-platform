"""Shared helpers for the PTP DAGs (ADR-0008, ADR-0009).

DAGs only *launch* versioned jobs from the ptp-jobs image. All logic lives in the Scala code, so the
same job runs from `make` and from Airflow. Secrets go through `private_environment`, which is
neither templated nor shown in the UI.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

JOBS_IMAGE = os.environ.get("PTP_JOBS_IMAGE", "ptp-jobs:local")
DOCKER_URL = os.environ.get("PTP_DOCKER_URL", "tcp://docker-proxy:2375")
NETWORK = os.environ.get("PTP_DOCKER_NETWORK", "ptp_default")
HOST_DATA_DIR = os.environ.get(
    "PTP_HOST_DATA_DIR", ""
)  # absolute host path of the repo's ./data (set by make)

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=45),
}

_SECRET_FOR = {"batch": "PTP_BATCH_WRITER_PASSWORD", "stream": "PTP_STREAM_WRITER_PASSWORD"}


def spark_job(task_id: str, command: list[str], writer: str = "batch", **kwargs: Any) -> DockerOperator:
    """One short-lived ptp-jobs container on the Compose network, with the lake mounted."""
    if not HOST_DATA_DIR.startswith("/"):
        raise ValueError("PTP_HOST_DATA_DIR must be the absolute host path of ./data (run `make env`)")
    secret = _SECRET_FOR[writer]
    return DockerOperator(
        task_id=task_id,
        image=JOBS_IMAGE,
        command=command,
        docker_url=DOCKER_URL,
        network_mode=NETWORK,
        # Same path inside and outside: the bronze sink log stores absolute file paths.
        mounts=[Mount(source=f"{HOST_DATA_DIR}/lake", target=f"{HOST_DATA_DIR}/lake", type="bind")],
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
        mem_limit="2g",
        environment={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "POSTGRES_JDBC_URL": f"jdbc:postgresql://postgres:5432/{os.environ.get('POSTGRES_DB', 'ptp')}",
            "LAKE_ROOT": f"{HOST_DATA_DIR}/lake",
            "DRIVER_MEMORY": "1536m",
            "BRONZE_RETENTION_DAYS": os.environ.get("BRONZE_RETENTION_DAYS", "90"),
            "SILVER_RETENTION_DAYS": os.environ.get("SILVER_RETENTION_DAYS", "400"),
            "PROCESSED_EVENTS_RETENTION_DAYS": os.environ.get("PROCESSED_EVENTS_RETENTION_DAYS", "30"),
        },
        private_environment={secret: os.environ.get(secret, "")},
        **kwargs,
    )

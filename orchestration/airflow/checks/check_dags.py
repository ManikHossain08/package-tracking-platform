"""TS-O-01 / AC-7.8: DAG integrity, run inside the Airflow container (no pytest in the image).

docker compose exec airflow python /opt/airflow/checks/check_dags.py
"""

from __future__ import annotations

import sys

from airflow.dag_processing.dagbag import DagBag

# Airflow's DAG processor puts the dags folder on sys.path (for ptp_common); do the same here.
sys.path.insert(0, "/opt/airflow/dags")

failures: list[str] = []


def check(ok: bool, message: str) -> None:
    print(("PASS " if ok else "FAIL ") + message)
    if not ok:
        failures.append(message)


bag = DagBag(dag_folder="/opt/airflow/dags")
check(not bag.import_errors, f"no import errors {dict(bag.import_errors) or ''}")

expected = {
    "lake_daily": {"silver": {"gold"}, "gold": {"dq"}, "dq": set()},
    "replay_from_bronze": {"validate": {"replay"}, "replay": set()},
    "maintenance": {"retention": set()},
}
for dag_id, edges in expected.items():
    dag = bag.dags.get(dag_id)
    check(dag is not None, f"{dag_id} loaded")
    if dag is None:
        continue
    check(set(dag.task_dict) == set(edges), f"{dag_id} tasks = {sorted(edges)}")
    for task_id, downstream in edges.items():
        task = dag.task_dict.get(task_id)
        if task is None:
            continue
        check(
            set(task.downstream_task_ids) == downstream, f"{dag_id}.{task_id} → {sorted(downstream) or 'end'}"
        )
        check(task.execution_timeout is not None, f"{dag_id}.{task_id} has an execution timeout")
        check(task.retries is not None, f"{dag_id}.{task_id} has retries set ({task.retries})")
        if task.task_type == "DockerOperator":
            check(task.image.startswith("ptp-jobs:"), f"{dag_id}.{task_id} runs the ptp-jobs image")
            check(
                "{{" in " ".join(task.command) or task_id in ("silver", "retention"),
                f"{dag_id}.{task_id} command is run-scoped ({' '.join(task.command)})",
            )
            check(
                not task.environment.keys() & {"PTP_BATCH_WRITER_PASSWORD", "PTP_STREAM_WRITER_PASSWORD"},
                f"{dag_id}.{task_id} keeps secrets out of templated env",
            )

replay = bag.dags.get("replay_from_bronze")
if replay is not None:
    check(
        set(replay.params.keys()) == {"ingest_date_from", "ingest_date_to", "mode", "dry_run"},
        "replay params",
    )
    check(replay.schedule is None or str(replay.timetable.summary) == "None", "replay is manual only")
check(
    bag.dags["lake_daily"].task_dict["dq"].retries == 0,
    "lake_daily.dq is not retried (findings are not transient)",
)

print(f"check_dags: {len(failures)} failure(s)")
sys.exit(1 if failures else 0)

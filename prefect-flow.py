"""DS 3022 Data Project 2: Prefect flow.

Your pipeline must, without human intervention:
  1. POST to the scatter API to populate your queue (once per run, never inside a retry loop)
  2. Monitor the queue (get_queue_attributes) and collect all 21 messages
  3. Land each fragment in DuckDB (raw.fragments) BEFORE deleting its message
  4. Run dbt: tests are the quality gate, the mart model holds the phrase
  5. Submit the phrase to the 'dp2-submit' queue and log the HTTP status code

Run:   python prefect-flow.py
Then:  python kit/check_submission.py
"""
import os
import time

import boto3
import duckdb
from dbt.cli.main import dbtRunner
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()

UVA_ID = os.environ["UVA_ID"]
SCATTER_URL = f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")

sqs = boto3.client("sqs")  # the endpoint comes from AWS_ENDPOINT_URL_SQS

COUNTER_NAMES = [
    "ApproximateNumberOfMessages",            # visible
    "ApproximateNumberOfMessagesNotVisible",  # in flight
    "ApproximateNumberOfMessagesDelayed",     # delayed
]


@task(retries=2, retry_delay_seconds=5)
def populate_queue() -> str:
    """POST to the scatter API and return your queue URL.

    Retrying is safe here: it only happens if the POST itself failed,
    before anything was collected. This task runs once per flow run.
    """
    logger = get_run_logger()
    resp = requests.post(SCATTER_URL, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    logger.info(f"Scatter API answered: {payload}")
    return payload["sqs_url"]


def get_counts(queue_url: str) -> dict:
    """Plain helper (not a task): return the three ApproximateNumberOf... counters as integers."""
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=COUNTER_NAMES
    )["Attributes"]
    return {
        "visible": int(attrs.get("ApproximateNumberOfMessages", 0)),
        "in_flight": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
        "delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
    }


@task
def monitor_queue(queue_url: str, poll_s: int = 5, timeout_s: int = 1200) -> dict:
    """Checkpoint A only: log the counters until nothing is delayed.
    No longer called by the flow: collect_messages watches the counters itself."""
    logger = get_run_logger()
    deadline = time.monotonic() + timeout_s
    while True:
        c = get_counts(queue_url)
        logger.info(f"visible={c['visible']} in_flight={c['in_flight']} delayed={c['delayed']}")
        if c["delayed"] == 0:
            logger.info("All messages released")
            return c
        if time.monotonic() > deadline:
            raise TimeoutError(f"Still {c['delayed']} delayed messages after {timeout_s} s")
        time.sleep(poll_s)


@task
def prepare_landing() -> None:
    """Create an empty raw.fragments for this puzzle (each POST starts a new one).

    Separate task on purpose: if collect_messages is ever retried it must not
    wipe rows whose messages are already deleted from the queue.
    order_no is stored as text, exactly as received; dbt casts it.
    """
    logger = get_run_logger()
    with duckdb.connect(DUCKDB_PATH) as con:
        con.execute("create schema if not exists raw")
        con.execute(
            "create or replace table raw.fragments "
            "(order_no varchar, word varchar, received_at timestamp)"
        )
    logger.info(f"Landing table raw.fragments ready in {DUCKDB_PATH}")


@task
def collect_messages(queue_url: str, expected: int = 21, timeout_s: int = 1200, wait_s: int = 10) -> int:
    """Receive, land in DuckDB, then delete. Return how many distinct fragments are stored.

    Polling strategy: every pass logs the three counters, then long-polls
    receive_message (up to wait_s seconds) for up to 10 messages.
    Stop rule: `expected` DISTINCT order_no stored AND all three counters at 0.
    """
    logger = get_run_logger()
    deadline = time.monotonic() + timeout_s
    with duckdb.connect(DUCKDB_PATH) as con:
        while True:
            stored = con.execute("select count(distinct order_no) from raw.fragments").fetchone()[0]
            c = get_counts(queue_url)
            logger.info(
                f"stored {stored}/{expected} | visible={c['visible']} "
                f"in_flight={c['in_flight']} delayed={c['delayed']}"
            )
            if stored >= expected and c["visible"] == c["in_flight"] == c["delayed"] == 0:
                logger.info(f"All {stored} fragments stored and the queue is empty")
                return stored
            if time.monotonic() > deadline:
                raise TimeoutError(f"Only {stored}/{expected} fragments after {timeout_s} s")

            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
                WaitTimeSeconds=wait_s,
            )
            for msg in resp.get("Messages", []):  # no 'Messages' key when nothing is visible
                attrs = msg["MessageAttributes"]
                order_no = attrs["order_no"]["StringValue"]
                word = attrs["word"]["StringValue"]
                # 1) save ...
                con.execute(
                    "insert into raw.fragments values (?, ?, current_timestamp)",
                    [order_no, word],
                )
                # 2) ... then delete, with the ReceiptHandle from THIS receive
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])


@task
def dbt_build() -> None:
    """Run `dbt build --project-dir dbt --profiles-dir dbt`; raise if anything fails."""
    logger = get_run_logger()
    res = dbtRunner().invoke(["build", "--project-dir", "dbt", "--profiles-dir", "dbt"])
    if not res.success:
        raise RuntimeError("dbt build failed: a data test did not pass, so the phrase will NOT be submitted")
    logger.info("dbt build passed: all tests green")


@task
def read_phrase() -> str:
    """Read the phrase from the dbt mart model in DuckDB."""
    logger = get_run_logger()
    con = duckdb.connect(DUCKDB_PATH)
    try:
        phrase, n = con.execute("select phrase, n_fragments from assembled_phrase").fetchone()
    finally:
        con.close()
    logger.info(f"Assembled {n} fragments: {phrase}")
    return phrase


@task
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    """Send to dp2-submit with uvaid / phrase / platform attributes. Return the HTTP status code."""
    # TODO (Checkpoint D)
    raise NotImplementedError


@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline(timeout_s: int = 1200):
    queue_url = populate_queue()
    prepare_landing()
    collect_messages(queue_url, timeout_s=timeout_s)
    dbt_build()
    phrase = read_phrase()

if __name__ == "__main__":
    dp2_pipeline()
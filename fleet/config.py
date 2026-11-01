"""Settings, all read from environment variables so the same code runs locally (moto) or on AWS."""
from __future__ import annotations

import os

import boto3
from botocore.config import Config

DEFAULT_PG_DSN = "host=127.0.0.1 port=5432 user=fleet password=fleet dbname=fleet"
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")


def pg_dsn() -> str:
    return os.environ.get("FLEET_PG_DSN", DEFAULT_PG_DSN)


def pg_schema() -> str | None:
    """Optional PostgreSQL schema (the tests use their own so they never touch real data)."""
    return os.environ.get("FLEET_PG_SCHEMA") or None


def aws_endpoint() -> str | None:
    """Set AWS_ENDPOINT_URL to a moto server for local runs; leave it unset to use real AWS."""
    return os.environ.get("AWS_ENDPOINT_URL") or None


def queue_url() -> str:
    return os.environ["FLEET_QUEUE_URL"]


def table_name() -> str:
    return os.environ.get("FLEET_TABLE", "fleet-robot-state")


def boto_session() -> boto3.session.Session:
    # moto accepts any credentials; real AWS uses your normal credential chain.
    if aws_endpoint():
        return boto3.session.Session(aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "testing"),
                                     aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "testing"),
                                     region_name=REGION)
    return boto3.session.Session(region_name=REGION)


def client(service: str, endpoint: str | None = None, fail_fast: bool = False):
    """A boto3 client. fail_fast: one attempt and short timeouts, so callers see an outage at once."""
    cfg = Config(retries={"max_attempts": 1 if fail_fast else 3, "mode": "standard"},
                 connect_timeout=1 if fail_fast else 5, read_timeout=5 if fail_fast else 30,
                 max_pool_connections=32)
    return boto_session().client(service, endpoint_url=endpoint or aws_endpoint(), config=cfg)


def resource(service: str, endpoint: str | None = None):
    cfg = Config(retries={"max_attempts": 3, "mode": "standard"}, max_pool_connections=32)
    return boto_session().resource(service, endpoint_url=endpoint or aws_endpoint(), config=cfg)

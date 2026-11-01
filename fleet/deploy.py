"""Deploys the CloudFormation stack and creates the SQL schema, then prints the settings to export.

    python -m fleet.deploy            # to AWS_ENDPOINT_URL (moto) or, if unset, to your AWS account
"""
from __future__ import annotations

import os

from . import config, db, infra


def main() -> None:
    if config.aws_endpoint():
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    out = infra.deploy_stack(config.client("cloudformation"))
    with db.connect(config.pg_dsn(), schema=config.pg_schema()) as conn:
        db.create_schema(conn)
    print(f"export FLEET_QUEUE_URL={out['QueueUrl']}")
    print(f"export FLEET_TABLE={out['TableName']}")
    print(f"# dead-letter queue: {out['DeadLetterQueueUrl']}")


if __name__ == "__main__":
    main()

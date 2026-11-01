# Robot Fleet Telemetry on AWS: SQS, DynamoDB and PostgreSQL

[![fleet-ci](https://github.com/vansh0906/fleet-telemetry-aws/actions/workflows/ci.yml/badge.svg)](https://github.com/vansh0906/fleet-telemetry-aws/actions/workflows/ci.yml)

A **distributed telemetry pipeline for a fleet of warehouse robots**. Robots upload status reports to a
**FastAPI** ingest service, which queues them in **Amazon SQS**. **Queue workers** store the full history in
**PostgreSQL** (SQL) and keep each robot's **latest state in DynamoDB** (NoSQL). The AWS resources are defined
in a **CloudFormation template**.

The point of the project is **correctness under failure**. SQS delivers every message **at least once**,
robots re-send data when an acknowledgement is lost, reports arrive out of order, and workers and databases
fail. The pipeline is built so that none of this loses a report, stores one twice, or leaves a robot's
state stale. Every claim is checked by an experiment that injects the fault.

```text
 robots (20, simulated)        ingest API                SQS queue            workers (4)            PostgreSQL
 store-and-forward  ──POST──▶  FastAPI, pydantic  ──▶   at-least-once   ──▶  idempotent insert ──▶ full history, SQL
 retry on 503                  validation (422)          redrive after 3      ON CONFLICT          (robot_id, ts) index
                               503 if SQS is down        receives             DO NOTHING
                                                              │                    │
                                                              ▼                    ▼
                                                      dead-letter queue      conditional write ──▶ DynamoDB
                                                      (poison messages)      "only if seq newer"   latest state per robot
```

## How it works

| Part | File | What it does |
|---|---|---|
| Message format | `fleet/models.py` | **pydantic** models: robot id, sequence number, position, battery, state, fault code. Rejects NaN, out-of-range values and a FAULT without a code |
| Ingest API | `fleet/api.py` | `POST /telemetry` validates a batch and sends it to SQS with `SendMessageBatch` (10 per call, failed entries retried). Returns **503** if SQS cannot be reached, so the robot keeps its data and retries. Query endpoints read DynamoDB and PostgreSQL |
| Worker | `fleet/worker.py` | Long-polls SQS, inserts into PostgreSQL with **`ON CONFLICT (event_id) DO NOTHING`**, writes DynamoDB with a **conditional expression** (`seq` must be newer), and **deletes the message only after both succeeded**. A **circuit breaker** stops taking messages while the database is down |
| Supervisor | `fleet/worker.py` (`WorkerPool`) | Replaces a worker that crashes |
| SQL | `fleet/db.py` | Schema with constraints, idempotent batch insert, `DISTINCT ON` latest-state query, fleet summary, robot history |
| Infrastructure | `infra/fleet-stack.yaml`, `fleet/infra.py` | **CloudFormation**: SQS queue, dead-letter queue (redrive after 3 receives), DynamoDB table keyed on `robot_id`. Plus a DLQ **redrive** tool |
| Robots | `fleet/robots.py` | Simulated robots with a motion and battery model, store-and-forward buffering, re-sends after lost acknowledgements, and late reports. Their ledger is the ground truth |
| Fault injection | `fleet/chaos.py` | A **TCP proxy that can cut a network link** (API to SQS, workers to PostgreSQL) so outages are real connection failures |
| Experiments | `run_all.py` | Runs everything against a local **moto** server (AWS emulator) and a real PostgreSQL, and prints the requirement table |
| Tests | `tests/` | 20 tests: validation, message parsing, the proxy, the CloudFormation stack, worker idempotency, crash recovery, DLQ and API |

### Why each step is idempotent

| Failure | What happens | Why nothing breaks |
|---|---|---|
| Robot re-sends a batch (lost ack) | Same `event_id` reaches the worker twice | `ON CONFLICT (event_id) DO NOTHING` stores it once |
| Worker dies after the database commit | Message is not deleted, so SQS delivers it again after the visibility timeout | Insert is a no-op; the DynamoDB write runs again and is idempotent |
| Report arrives late | An older report is processed after a newer one | DynamoDB `ConditionExpression: attribute_not_exists(robot_id) OR seq < :seq` refuses it; history still gets it |
| Message can never be parsed | Worker leaves it; SQS delivers it again | After 3 receives SQS moves it to the **dead-letter queue** |
| PostgreSQL down | Insert fails | **Circuit breaker**: the worker stops receiving until the database answers, so healthy messages do not use up their 3 receives and land in the DLQ |
| SQS unreachable from the API | `SendMessageBatch` fails | API returns 503; the robot keeps the reports and retries with backoff |

## Results

<!-- results:start -->
```text
PostgreSQL: user=fleet password=*** dbname=fleet host=127.0.0.1 port=5432
AWS: local moto server (SQS, DynamoDB, CloudFormation); no AWS account used

[1  Fleet run: 20 robots x 3 Hz x 20 s, 4 workers]
   faults injected: 3 % lost acks (re-sent batches), 5 % reports held back 1-3 s, worker crash on 2 % of batches, 20 poison messages
   reports 1199, acknowledged 1199, not acknowledged 0
   rows in PostgreSQL 1199 (distinct 1199): missing 0, extra 0
   messages received 1356: inserted 1199, duplicates absorbed 97, poison receives 60
   duplicate sources: 40 re-sent batches, 29 worker crashes (replaced by the supervisor)
   latest state in DynamoDB: conditional write 20/20 robots correct, plain last-write-wins 9/20; out-of-order writes refused 102
   dead-letter queue: 20 poison, 0 valid reports
   robot -> PostgreSQL latency (on-time reports, n=1148): p50 142 ms, p95 276 ms, p99 438 ms, max 0.67 s
   incl. held-back reports: p95 441 ms; load 60 reports/s; queue depth max 38, median 9; queue empty 1.9 s after the last upload
   /fleet/summary vs ground truth: 6/6 fields match (by state {'FAULT': 2, 'MOVING': 18}, low battery 5 robots, fault reports 43)
   /robots/AR-0001 -> seq 60 MOVING battery 27.9 %

[2  API cannot reach SQS for 3 s (25 robots, 12 s run)]
   uploads 860: failed 55 (503 or connection refused), robots buffered up to 21 reports each
   reports 1200, stored 1200, missing 0, never acknowledged 0; duplicates absorbed 2

[3  PostgreSQL unreachable for 5 s (15 robots, 12 s run, 4 workers)]
   naive retry    : 720 reports, DB errors 498, receives 1614, valid reports dead-lettered 298, missing 298 -> 0 after redriving 298 from the DLQ
   circuit breaker: 720 reports, DB errors 4, receives 724, valid reports dead-lettered 0, missing 0

[4  SQL index: one robot's last 100 reports from 400,000 rows]
   without index: 7.11 ms (Seq Scan); with index on (robot_id, ts): 0.016 ms (Index Scan)
   table 57 MB, index 12 MB; speed-up 444x

[5  Infrastructure and API checks]
   CloudFormation stack deployed in 0.4 s: queue -> DLQ after 3 receives, visibility 2 s, table key robot_id
   malformed uploads rejected with 422: 7/7

ID   Requirement                                         Result                        Target                      Status
R1   Every acknowledged report stored exactly once       0 missing, 0 extra            0, 0                        PASS
R2   Duplicate deliveries absorbed (re-sends + crashes)  97 absorbed                   > 0, R1 holds               PASS
R3   Latest state right despite late reports             20/20 (naive 9)               all                         PASS
R4   Poison messages in DLQ, no valid ones               20/20, 0 valid                20/20, 0                    PASS
R5   p95 robot -> database latency                       276 ms                        <= 1000 ms                  PASS
R6   SQS unreachable 3 s: no report lost                 0 lost, 55 failed uploads     0 lost, > 0 failed          PASS
R7   DB down 5 s: breaker keeps data out of DLQ          0 in DLQ (naive 298)          0, 0 missing                PASS
R8   Workers keep up: queue depth bounded                max 38, empty 1.9 s after     <= 200, <= 10 s             PASS
R9   Fleet summary SQL matches ground truth              6/6 fields                    all                         PASS
R10  Index speeds up robot history query                 444x, Index Scan              >= 10x, index used          PASS
R11  CloudFormation stack: queue, DLQ, table             as specified                  maxReceive 3, key robot_id  PASS
R12  Malformed uploads rejected (HTTP 422)               7/7                           all                         PASS
12/12 requirements pass
```
<!-- results:end -->

## Run it (macOS)

You need **Python 3.12** and **Docker Desktop** (for PostgreSQL).

```bash
docker compose up -d                       # PostgreSQL 16 on localhost:5432 (user/password fleet)
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export FLEET_PG_DSN="host=127.0.0.1 port=5432 user=fleet password=fleet dbname=fleet"
python -m unittest discover -s tests -t . -v    # 20 tests
python run_all.py | tee results.txt             # all experiments, about 3 minutes
python tools/readme_results.py                  # copies results.txt into this README
```

No AWS account is needed: `run_all.py` starts a **moto** server that emulates SQS, DynamoDB and
CloudFormation on your machine.

Run the services by hand instead (each command in its own terminal, with the venv active and
`FLEET_PG_DSN` set):

```bash
# terminal 1: local AWS emulator
python -m moto.server -p 5001

# terminals 2 and 3: point at it and deploy (safe to repeat: an existing stack is reused)
export AWS_ENDPOINT_URL=http://127.0.0.1:5001
eval "$(python -m fleet.deploy)"

uvicorn fleet.api:app --port 8000          # terminal 2, then open http://127.0.0.1:8000/docs
python -m fleet.worker --workers 4         # terminal 3
```

(Port 5000 is taken by AirPlay on macOS, hence 5001.)

### On real AWS (optional, not tested)

The same code runs against AWS when `AWS_ENDPOINT_URL` is unset: deploy `infra/fleet-stack.yaml` with
`aws cloudformation deploy --template-file infra/fleet-stack.yaml --stack-name fleet-telemetry`, point
`FLEET_PG_DSN` at a PostgreSQL instance (for example RDS), and set `FLEET_QUEUE_URL` from the stack outputs.
This path has **not** been run on AWS; SQS and DynamoDB on-demand cost little at this volume, but delete the
stack afterwards (`aws cloudformation delete-stack --stack-name fleet-telemetry`).

## Limitations

- **AWS is emulated** by moto. The SQS and DynamoDB semantics used here (visibility timeout, receive count,
  redrive, conditional writes) behave as on AWS, but **latency and throughput are not AWS numbers**. The
  emulator is the bottleneck: its SQS calls slow down as the queue grows, and at 80 reports/s the queue backed
  up (p95 latency above 5 s), so the experiments run at 60 reports/s. For the same reason the project does not
  claim a speed-up from adding workers.
- Latency compares the robot's clock with the database clock. Both run on one computer here; a real fleet
  needs synchronised clocks (NTP or PTP) or must measure from the API's `accepted_at`.
- Worker crashes are simulated by ending a worker thread between the commit and the delete; the supervisor
  starts a replacement, as a container orchestrator would restart a task.
- A message is one report. Real fleets often pack many reports into one message to cut the per-message cost.

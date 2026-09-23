# delivery-eta-mesh

[![CI](https://github.com/santiago-ven-ai/delivery-eta-mesh/actions/workflows/ci.yml/badge.svg)](https://github.com/santiago-ven-ai/delivery-eta-mesh/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-9%25-yellow)](https://github.com/santiago-ven-ai/delivery-eta-mesh/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

An event-driven ETA recomputation mesh for food delivery dispatch — a Spring Boot scoring worker on Fargate, with late-event handling and Spark skew handling for hot restaurants.

## Pitch Card

**Problem** — Food delivery dispatch degrades when courier GPS events arrive late or out of order; stale ETAs cause bad courier assignments, late orders, and refund costs.

**Solution** — An event-driven ETA mesh: a Spring Boot scoring worker on Fargate consuming SQS, watermark-based handling of late GPS events, and Spark key-salting for the small set of restaurants that generate most of the traffic.

**Impact** — Worker verified end-to-end against real SQS + DynamoDB (5 orders sent → 5 scored; a redelivered order_id overwrites instead of duplicating). Partition imbalance from the top-5%-of-restaurants skew (measured at 59.5% of all orders) reduced from **11.5x max/min partition size to 1.7x** after salting the shuffle key.

**Stack** — Python 3 · PySpark · FastAPI · Java 17 / Spring Boot 4.1 · AWS (SNS, SQS, ECS/Fargate, EC2, DynamoDB, S3) via MiniStack

---

## Architecture

```
  src/ingestion/data_gen.py
    order events + courier GPS pings (EC2-daemon equivalent)
             |
             v
  src/ingestion/publisher.py
             |
             v
        SNS dispatch-events
             |
             v
   SQS scoring queue  (+ DLQ on failure)
             |
             v
  src/worker/.../EtaScoringWorker.java (Spring Boot on Fargate)
    consume batch --> compute ETA
             |
             v
   DynamoDB eta-current
   (worker only ever writes the CURRENT eta — history
    correction happens only in the nightly replay below)

  all raw events also land in --> S3 (partitioned)
             |
             v
  nightly src/transformation/replay.py (PySpark)
    watermark late GPS arrivals, salt hot restaurant keys
             |
        +----+----+
        v         v
  S3 dispatch-agg/   scripts/accuracy.py --write
  order_counts       --> S3 dispatch-agg/eta_accuracy
        |                 |
        +--------+--------+
                 v
     src/utils/warehouse.py :: DuckDB
       (Redshift stand-in, offline/warehouse queries only)

  src/serving/api.py :: FastAPI
    /eta/{order_id}   reads eta-current live
    /accuracy/daily   computes MAE live from eta-current + events
                       (not the persisted aggregate)
```

See `docs/architecture.md` for the diagram.

## Why Java/Spring Boot here

The scoring worker is a stateless, high-throughput SQS consumer where a warm JVM on Fargate outperforms Python cold starts, and Spring Boot provides SQS polling, health checks, and metrics without custom plumbing.

**Honesty note:** Java/Spring Boot is used as a bounded scoring worker — not evidence of Java platform seniority. The Python/PySpark/FastAPI layers remain the production-grade core.

## Measured in this repo

| Metric | Value | How it's measured |
|---|---|---|
| Skew concentration (top 5% of restaurants) | **59.5%** of all orders, on a 24h/100-restaurant/9,600-event synthetic run | `python3 src/ingestion/data_gen.py` |
| Partition imbalance, naive shuffle by `restaurant_id` | **11.52x** (max 1,440 rows vs. min 125 rows per partition) | `python3 src/transformation/replay.py` |
| Partition imbalance, salted shuffle by `(restaurant_id, salt)` | **1.7x** (max 839 vs. min 494) | `python3 src/transformation/replay.py` |
| Late GPS pings watermarked (not merged into live state) | 545 of 4,800 GPS events (11.4%) | `python3 src/transformation/replay.py` |
| Worker end-to-end: orders sent → rows scored in DynamoDB | **5/5**, real SQS → real Spring Boot worker → real DynamoDB | manual verification, see BUILD_GUIDE step 3 |
| Redelivery of the same `order_id` | **1 row**, not a duplicate — later message's ETA wins | manual verification |

> These numbers are honest about scale: on a single-machine `local[2]` Spark session, wall-clock speedup from salting isn't a reliable signal (no real cluster for a hot partition to bottleneck), so partition *balance* — the actual mechanism salting fixes — is what's reported instead of a timing number that would be noise at this scale.

## Modeled business impact (synthetic data — assumptions documented)

| Assumption | Source | Modeled outcome |
|---|---|---|
| Refund/support cost per late-order incident, incidents avoided by improved ETA MAE | TODO — cite in `docs/impact-model.md` | TODO |

## Emulated vs. real

| Component | Dev (this repo) | Production would use | Fidelity |
|---|---|---|---|
| S3 / SNS / SQS / DynamoDB | [MiniStack](https://ministack.org) (free, MIT, no account) | AWS | High |
| AWS CLI v2 | Real `aws` CLI against MiniStack (`AWS_ENDPOINT_URL`) — see `docs/RUNBOOK.md` §2. Note: this repo doesn't use Lambda or Step Functions, unlike the other 4 | AWS CLI v2 | High |
| ECS / Fargate | MiniStack ECS — **launches the Spring Boot worker as a real Docker container from the actual task definition** | AWS Fargate | Medium-High — same task def, no real autoscaling |
| ECR | MiniStack — `make deploy-ecs` builds the worker image, pushes it to a real ECR repo (`docker push localhost:4566/eta-worker`), then resolves `ecs/task-definition.json.template`'s image reference before registering the task. Verified live: MiniStack's registry is reachable at two different hostnames depending on caller (`localhost:4566` from the host, `ministack:4566` from inside another container) and resolves both to the same image | Amazon ECR | High — `describe-images` reflects the real pushed digest, the ECS task genuinely pulls it |
| EC2 | MiniStack EC2 (API only) + simulator daemon as a compose service | EC2 | Low — API only; user-data script shipped for reference |
| Redshift | **DuckDB**, reading aggregate Parquet directly from S3 | Redshift Serverless | Medium — no MPP distribution; no `sql/redshift/` DDL exists in this repo (see `src/utils/warehouse.py`'s docstring) |

## Three non-tutorial challenges

1. **Late, out-of-order GPS events** — watermarking plus an explicit policy: rewrite historical ETA or just flag it as stale?
2. **Spark skew** — the top 5% of restaurants generate ~60% of events; key-salting with a measured before/after speedup.
3. **Real cost comparison** — the same workload on Fargate vs. Lambda, with a $/million-events table and the volume at which the cost curve crosses.

## Demo (3 minutes)

```bash
source env.sh
make demo        # 2h × 10 restaurants × 20 orders/h — learn (docs/RUNBOOK.md)
make demo-full   # full synthetic dispatch day (24×100×200)
pytest tests/integration/test_worker_idempotency.py   # redelivery-does-not-duplicate check, standalone
make query
```

## What this is NOT

Not an "Uber ETA clone" tutorial. What sets it apart: late-event handling, measured skew mitigation, and an explicit cost comparison — not just a happy-path prediction demo.

## Build it yourself

See [`docs/RUNBOOK.md`](docs/RUNBOOK.md) to run the flow, or [`docs/BUILD_GUIDE.md`](docs/BUILD_GUIDE.md) to build from scratch.

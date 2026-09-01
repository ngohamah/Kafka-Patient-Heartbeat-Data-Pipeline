# Real-Time Customer Heart Beat Monitoring System

A small data-engineering pipeline that simulates heart-rate readings for a
set of customers, streams them through Kafka, validates them, and stores
them in PostgreSQL for querying and (optionally) dashboarding.

> Full setup instructions, architecture diagram, and screenshots are added
> as the corresponding pipeline stages are built — see
> [implementationmap.md](implementationmap.md) for the current build phase.

## What this system does (plain language)

Imagine a hospital or fitness app with many customers wearing a heart-rate
monitor. Each device reports a reading (beats per minute) at regular
intervals. This project reproduces that flow end to end without needing
real hardware:

1. **A simulator** invents realistic-looking heart-rate readings for a pool
   of fake customers, the same way real monitors would send them.
2. **A streaming layer (Kafka)** carries those readings in real time, the
   same way a message queue would in a production system.
3. **A checking step** looks at each reading and flags anything medically
   implausible (for example, a reading of 5 or 400 beats per minute) before
   it is stored, and writes down *why* anything was flagged.
4. **A database (PostgreSQL)** keeps a permanent, searchable record of every
   valid reading, indexed by time so recent activity is fast to look up.
5. **(Optional) A dashboard** turns the stored data into readable charts.

## Project layout

```
src/                 Python source (generator, producer, consumer, db, config, logging)
sql/                 Database schema
tests/               Unit tests
docs/                Architecture diagram and screenshots
dashboard/           Optional dashboard (Grafana provisioning or Streamlit app)
docker-compose.yml   Local Kafka + PostgreSQL (+ Grafana) stack
```

## Prerequisites

- Python 3.11+
- Docker and Docker Compose (to run Kafka + PostgreSQL locally)

## Setup (developer)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

## Running the local stack

```bash
docker-compose up -d
```

## Running tests and lint

```bash
pytest
ruff check .
```

## Status

See [implementationmap.md](implementationmap.md) for the current build phase
and design decisions (not tracked in git — ask the project owner for the
current roadmap if you need it).

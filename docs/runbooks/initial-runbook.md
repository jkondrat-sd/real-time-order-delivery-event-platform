# Initial runbook

This runbook explains how to bring up the local stack and run the event generator and a simple smoke test.

Prereqs:

- Docker Desktop (with Docker Compose v2)
- Python 3.10+

Quick start:

1. Export env file (optional):

```bash
cp local/.env .env
```

2. Start the stack:

```bash
make up
```

3. Generate 500 events (dry-run):

```bash
python services/event_generator/main.py --events 500 --dry-run
```

4. (Optional) Publish events to Redpanda:

```bash
python services/event_generator/main.py --events 1000 --topic orders.events --brokers redpanda:29092
```

Next steps:

- Wire PyFlink job to read from `orders.events` and write KPIs to `kpi.minute`.
- Add tests and CI to spin up the compose stack and validate end-to-end flow.

Conda (recommended)
-------------------

Use the provided Conda environment to manage dev dependencies:

```bash
conda env create -f environment.yml -n rtod-env
conda activate rtod-env
```

Or run the helper scripts in `./scripts/` to create the env on Linux/macOS or Windows PowerShell.
Or run the helper script on Linux/macOS:

```bash
./scripts/create_conda_env.sh
conda activate rtod-env
```

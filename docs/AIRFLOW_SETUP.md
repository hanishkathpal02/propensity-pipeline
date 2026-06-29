# Airflow on EC2 — setup runbook

Open-source Airflow on a single EC2 box, for this pipeline. Shape: **Docker Compose +
LocalExecutor + Postgres metadata DB** (parallel tasks on one machine, no Celery/Redis).

Airflow is a long-running server (scheduler + webserver + metadata DB) — it must run on
the always-on EC2, not a laptop. You manage it from anywhere via the web UI / SSH.

## 1. Install Docker + Compose

Ubuntu:
```bash
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker $USER && newgrp docker
```
Amazon Linux 2023:
```bash
sudo dnf install -y docker && sudo systemctl enable --now docker
sudo usermod -aG docker $USER && newgrp docker
DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}; mkdir -p $DOCKER_CONFIG/cli-plugins
curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o $DOCKER_CONFIG/cli-plugins/docker-compose && chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose
```

## 2. Get Airflow, slim to LocalExecutor

```bash
mkdir -p ~/airflow && cd ~/airflow
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
mkdir -p dags logs plugins config
echo "AIRFLOW_UID=$(id -u)" > .env
```
Edit `docker-compose.yaml`:
1. `AIRFLOW__CORE__EXECUTOR: LocalExecutor`
2. Delete the `redis`, `airflow-worker`, `flower` service blocks and any `redis` lines under `depends_on`.
3. Add the AWS provider:
   `_PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-apache-airflow-providers-amazon}`
   (For production, bake a custom image instead of `_PIP_ADDITIONAL_REQUIREMENTS`.)

## 3. Boot + secure

```bash
docker compose up airflow-init      # creates metadata DB + admin user
docker compose up -d                # scheduler + webserver
docker compose ps                   # verify healthy
```
- Open port **8080 to your IP only** (or SSH tunnel: `ssh -L 8080:localhost:8080 ec2-user@<ip>`).
- Change the default `airflow`/`airflow` admin password in the UI immediately.

## 4. Deploy this pipeline

```bash
# clone the repo somewhere on the box, then point Airflow at its dags/:
git clone <repo-url> ~/propensity_pipeline
ln -s ~/propensity_pipeline/dags/*.py ~/airflow/dags/     # or set dags_folder
# install the package into the Airflow image/env so workers can `import propensity`:
#   simplest: add `-e /opt/airflow/propensity_pipeline` to _PIP_ADDITIONAL_REQUIREMENTS,
#   or bake it into a custom image. Mount the repo as a volume in docker-compose.yaml.
```

## 5. Connection + env + grants

- **Connection** (UI → Admin → Connections): `redshift_default` — Amazon Redshift,
  host / db `dev` / user / password / port 5439.
- **Env vars** for the worker: `REDSHIFT_*_DEV`, `AWS_*`, `KLAVIYO_*`, and
  `PROPENSITY_ARTIFACTS_DIR` / `PROPENSITY_OUTPUT_DIR` (durable paths, not /tmp).
- **Redshift grants** for the connection's DB user (procs run SECURITY INVOKER — see
  the main README "Deploy to Airflow" section for the exact GRANT block, including
  `USAGE ON DATABASE shared_klaviyo` for the Klaviyo proc).

## 6. Run

- Trigger DAG `propensity_pipeline`. Each step is a node you can click into for logs.
- `sync_klaviyo` stays **dry-run** until you set `dry_run=False` in `dags/propensity_dag.py`.

## Notes
- LocalExecutor on one box → tasks share local disk, so `io/artifacts.py` uses local
  paths. If you move to distributed workers, switch the artifact store to S3.
- The `run_model`-heavy step (`train_score`) needs enough RAM for LightGBM over the
  feature frame (~6M rows). Size the instance accordingly or offload to ECS/Batch.
```

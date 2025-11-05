# s2u_order_management

## Railway Nightly Worker (Option A)

To run all nightly syncs on Railway using a single scheduled worker:

- Create a new Service from this repo (name it `worker-nightly`).
- Set Start Command to:

  bash -lc './s2u_project/nightly_worker.sh'

- Set Cron Schedule (UTC) to your preferred time, e.g. `0 4 * * *` for 04:00 UTC.
- Enable Serverless so the worker only runs when scheduled.
- Restart Policy: On Failure (Railway default is fine).
- Ensure env vars are set on the service: `DJANGO_SETTINGS_MODULE`, `DATABASE_URL`, `REDIS_URL`, `KORONA_BASE_URL`, `KORONA_ACCOUNT_ID`, `KORONA_USER`, `KORONA_PASSWORD`.
- Optional: override monthly window via `MONTHLY_DAYS` env var (defaults to 30).

The script will sequentially run:

1. `sync_stores`
2. `load_products --skip-csv`
3. `sync_stocks`
4. `sync_all_monthly_sales --days 30`

## Celery (Option B)

If you prefer a resident worker with a scheduler instead of Railway Cron:

1) Dependencies are already added (`celery`). Broker/result use `REDIS_URL`.

2) Start commands for two Railway services:

- celery-worker:
  
  bash -lc "/opt/venv/bin/celery -A s2u_project worker -l info --concurrency=${CELERY_CONCURRENCY:-2}"

- celery-beat:
  
  bash -lc "/opt/venv/bin/celery -A s2u_project beat -l info"

3) Ensure env vars exist on both services: `DJANGO_SETTINGS_MODULE`, `DATABASE_URL`, `REDIS_URL`, Korona creds.

4) The schedule in `settings.py` runs `inventory.tasks.nightly_full_sync` at 04:00 UTC daily. Adjust by changing `CELERY_BEAT_SCHEDULE` or set `MONTHLY_DAYS` env var.

## Auto-Refresh on Login

- When any authenticated user logs in, the app checks when the last full refresh completed and, if older than 12 hours, starts a background refresh (stores → products → stocks → monthly sales). The default interval can be overridden with `REFRESH_INTERVAL_MINUTES` (minutes; default `720`).
- While a refresh is in progress, the Dashboard shows a progress bar. Admins (staff/superusers) see a “Refresh Data” button and can now also cancel an ongoing refresh from the same notice.

Endpoints involved (login required):
- `POST /api/refresh/start/` – start a full refresh.
- `GET  /api/refresh/status/?job=<id>` – poll job status.
- `POST /api/refresh/cancel/` – request cancellation; it takes effect between phases.

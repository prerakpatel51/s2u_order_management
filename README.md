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

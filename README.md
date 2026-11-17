# Fieldhouse pipeline

A generic R pipeline for TrackMan and TruMedia data. It pulls new sessions, cleans and types them, de-duplicates, writes Parquet to a private Hugging Face dataset, and builds summary tables.

This repo holds **code only**. There's no data, no team configuration, no credentials and no models. Configuration is read at run time from the private dataset (`config/config.yml`). Model scoring and the app live elsewhere.

## Workflows
| Workflow | When | What |
|---|---|---|
| live-sync | every 15 min | 5-second check for new TrackMan sessions; if there are any, pull, clean and add them to the live file |
| nightly-sync | nightly | re-check recent sessions, pull TruMedia, merge live data into season files, rebuild summaries, health check |
| backfill | manual | pull a date range |
| data-guard | every push | fails if a data file or credential is ever committed |

## Required settings (Settings → Secrets and variables → Actions)
- Variables: `HF_REPO`, `PRIVATE_REPO`
- Secrets: `HF_TOKEN`, `TRACKMAN_CLIENT_ID`, `TRACKMAN_CLIENT_SECRET`, `TRUMEDIA_USERNAME`, `TRUMEDIA_SITENAME`, `TRUMEDIA_MASTER_TOKEN`, `PRIVATE_DISPATCH_TOKEN`
- Optional: `TRACKMAN_FTP_USER`, `TRACKMAN_FTP_PASSWORD`

Logs contain counts and opaque session IDs only. Error text is redacted, and derived tokens are masked.

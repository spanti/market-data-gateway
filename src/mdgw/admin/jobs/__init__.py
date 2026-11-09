"""Admin Backfill Jobs (ticket 06).

The API validates, authorizes, audits and persists a job, then asks the
Collector through the allow-listed control plane to execute it. The Collector
is the only Dataset writer; the API never writes Parquet or the HWM.
"""

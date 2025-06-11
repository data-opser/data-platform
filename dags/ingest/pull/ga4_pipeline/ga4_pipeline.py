"""Loads data from gxr API"""
import logging
import os
from datetime import datetime

import dlt
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.destinations.adapters import bigquery_adapter

from google.cloud import bigquery

from airflow.models import Variable

# ─────────────────────── GA4 GCS constants ────────────────────────
GA4_TABLE = "bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*"

logger = logging.getLogger(__name__)

# ─────────────────────── GA4 sample-ecommerce loader ────────────────────────
@dlt.source
def ga4_source(initial_load_start: str = "2020-11-01"):
    """
    GA4 sample‑ecommerce loader that writes daily Parquet files to GCS.

    Each pipeline run processes **exactly one** day of data from
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    and advances the bookmark stored in `dlt.current.state()["ga4_date"]`.
    """
    print('step 0')
    bq_client = bigquery.Client()
    @dlt.resource(
        write_disposition="append",
    )
    def ga4_events():
        # Determine which date to pull
        print('step 1')

        run_date = ensure_pendulum_datetime(
            dlt.current.state().setdefault(
                "ga4_date", pendulum.parse(initial_load_start)
            )
        ).date()

        print('step 2')

        logger.info("GA4 run_date: %s", run_date)

        query = f"""
            SELECT *
            FROM `{GA4_TABLE}`
            WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @run_date)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_date", "DATE", run_date)
            ]
        )

        # Yield rows for dlt to load
        for row in bq_client.query(query, job_config=job_config).result():
            yield dict(row)

        # Advance checkpoint by one day
        next_date = pendulum.date(run_date.year, run_date.month, run_date.day).add(days=1)
        dlt.current.state()["ga4_date"] = next_date.isoformat()

    # Configure GCS Parquet destination
    bigquery_adapter(
        ga4_events,
        partition="event_date",
        cluster="event_name"
    )

    return ga4_events

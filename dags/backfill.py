# USAGE: A simple dag that allows users to run backfills utilizing the Airflow API rather than the CLI so the scheduler and it's rules are not ignored.
# Make sure to define the ASTRO_API_TOKEN environment variable.
import json

import logging
import os
from datetime import datetime, timedelta
from typing import List

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Param

logger = logging.getLogger(__name__)

DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY = timedelta(minutes=5)


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": DEFAULT_RETRIES,
        "retry_delay": DEFAULT_RETRY_DELAY,
    },
    tags=["backfill", "utility"],
    render_template_as_native_obj=True,
    params={
        "backfill_start_date": Param(
            type="string",
            format="date-time",
            description="Start date for backfill (YYYY-MM-DD)",
            default="2024-01-01",
        ),
        "backfill_end_date": Param(
            type="string",
            format="date-time",
            description="End date for backfill (YYYY-MM-DD)",
            default="2024-01-02",
        ),
        "backfill_dag_id": Param(
            type="string", description="DAG to backfill", default="tutorial"
        ),
        "deployment_url": Param(
            type="string",
            description="Astro Deployment URL",
            default="http://webserver:8080/",
        ),
    },
)
def backfill():
    @task
    def get_dates_to_backfill(
        backfill_start_date: str, backfill_end_date: str
    ) -> List[str]:
        """Generate list of dates to backfill.

        :param backfill_start_date: Start date for backfill (YYYY-MM-DD HH:mm:ss)
        :param backfill_end_date: End date for backfill (YYYY-MM-DD HH:mm:ss)
        :return: List of dates in ISO format
        """
        start_date = datetime.fromisoformat(backfill_start_date)
        end_date = datetime.fromisoformat(backfill_end_date)
        dates = []
        current_date = start_date

        while current_date <= end_date:
            dates.append(current_date.strftime("%Y-%m-%dT%H:%M:%SZ"))
            current_date += timedelta(days=1)

        logger.info(f"Generated {len(dates)} dates for backfill")
        return dates

    @task
    def trigger_backfill(backfill_logical_date: str, params: dict) -> dict:
        """Trigger backfill for a specific date using the Airflow API.

        :param backfill_logical_date: Date to trigger backfill
        :param params: DAG parameters containing dag_id and deployment_url
        :return: API response data
        :raises AirflowException: If API request fails
        """
        dag_id = params["backfill_dag_id"]
        deployment_url = params["deployment_url"].strip("/")
        token = os.environ.get("ASTRO_API_TOKEN")

        if not token:
            raise AirflowException("ASTRO_API_TOKEN environment variable is not set")

        try:
            response = requests.post(
                url=f"{deployment_url}/api/v1/dags/{dag_id}/dagRuns",
                data=json.dumps({"logical_date": backfill_logical_date}),
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                timeout=30,
            )
            response.raise_for_status()
            result = response.json()
            logger.info(f"Successfully triggered backfill for {backfill_logical_date}")
            return result

        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to trigger backfill: {str(e)}"
            logger.error(error_msg)
            raise AirflowException(error_msg)

    # DAG workflow
    dates_to_backfill = get_dates_to_backfill(
        backfill_start_date="{{ params.backfill_start_date }}",
        backfill_end_date="{{ params.backfill_end_date }}",
    )

    trigger_backfill.expand(backfill_logical_date=dates_to_backfill)


# Instantiate the DAG
backfill()

import os
import asyncio
from datetime import datetime, timedelta
from dap.api import DAPClient, DAPSession
from dap.dap_types import Credentials, Format, SnapshotQuery, IncrementalQuery, ProcessingError, JobStatus
import logging
logger = logging.getLogger(__name__)


class JobNotDone(Exception):
    pass


async def get_table_list(api_base_url: str, credentials: Credentials, namespace: str):
    """
    Fetches the list of tables from the DAP API.

    Args:
        api_base_url (str): The base URL of the DAP API.
        credentials (Credentials): The credentials for the DAP API.
        namespace (str): The namespace to fetch tables from.

    Returns:
        list: A list of table names.
    """
    async with DAPClient(
            base_url=api_base_url,
            credentials=credentials,
    ) as session:
        return await session.get_tables(namespace)


async def start_job_list(namespace: str, table_state_list: list[tuple[str, str, datetime]],
                         api_base_url: str, credentials: Credentials) -> list[dict[str, str]]:
    """
    Runs jobs for a list of tables.

    Args:
        namespace (str): The namespace of the tables.
        table_state_list (list): A list of tuples containing table name, status, and last updated timestamp.
        api_base_url (str): The base URL of the DAP API.
        credentials (Credentials): The credentials for the DAP API.

    Returns:
        list: A list of dictionaries containing table information and job IDs.
    """
    job_list = []
    async with DAPClient(api_base_url, credentials) as session:
        coros = [start_job(namespace, table_state[0], table_state[1], table_state[2], session) for table_state in table_state_list]
        results = await asyncio.gather(*coros)

        for result, table_state in zip(results, table_state_list):
            table, status, last_updated = table_state

            if result == "ProcessingError":
                status += "_pe"

            job_list.append({"table": table,
                             "state": status,
                             "last_updated": last_updated.strftime("%m/%d/%Y %H:%M:%S"),
                             "dap_job_id": result})

    return job_list


async def start_job(namespace: str, table_name: str, status: str, last_updated: datetime, session: DAPSession) -> str:
    """
    Submits a job to the DAP API to process a table.

    Args:
        namespace (str): The namespace of the table.
        table_name (str): The name of the table.
        status (str): The status of the table.
        last_updated (datetime): The last updated timestamp of the table.
        session (DAPSession): The DAP session.

    Returns:
        str: The job ID or "ProcessingError" if an error occurred.
    """
    logger.info(f"getting status of {table_name}")

    try:
        # __execute_query would be cleaner but is private despite calling two public methods
        if status == "needs_init":
            query = SnapshotQuery(format=Format.Parquet, mode=None)
            job = await session.query_snapshot(namespace, table_name, query)
        else:
            # for updates we give 1hr overlap time. process must succeed w/i 1hr or fail.
            query = IncrementalQuery(since=last_updated - timedelta(hours=1), until=None, format=Format.Parquet,
                                     mode=None)
            job = await session.query_incremental(namespace, table_name, query)

        job_id = job.id
    except ProcessingError:
        logger.warning(f"Processing error received on table: {table_name}")
        job_id = "ProcessingError"

    logger.info(job_id)

    return str(job_id)


async def download_job_output(job_id: str, base: str, c: Credentials, output_directory: str = "/tmp") -> list[str]:
    async with DAPClient(base, c) as session:
        logger.info(f"getting job {job_id}")
        job = await session.get_job(job_id)

        if job.status is not JobStatus.Complete:
            raise JobNotDone

        objects = job.objects
        directory = os.path.join(output_directory, f"job_{job_id}")

        logger.info("getting " + str(objects))
        downloaded_files = await session.download_objects(objects, directory, False)
        logger.info(f"Files from server downloaded to folder: {directory}")

        return downloaded_files

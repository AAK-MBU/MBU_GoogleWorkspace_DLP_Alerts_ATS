"""
This is the main entry point for the process
"""

import asyncio
import logging
import sys
import os

from dotenv import load_dotenv

from automation_server_client import AutomationServer, Workqueue

from mbu_dev_shared_components.database.connection import RPAConnection

from mbu_rpa_core.exceptions import BusinessError, ProcessError
from mbu_rpa_core.process_states import CompletedState

from helpers import ats_functions, config
from processes.application_handler import close, reset, startup
from processes.error_handling import handle_error
from processes.finalize_process import finalize_process
from processes.process_item import process_item
from processes.queue_handler import concurrent_add, retrieve_items_for_queue


# ╔══════════════════════════════════════════════╗
# ║ 🔥 REMOVE BEFORE DEPLOYMENT (TEMP OVERRIDES) 🔥 ║
# ╚══════════════════════════════════════════════╝
# # This block disables SSL verification and overrides env vars
# import requests
# import urllib3
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# _old_request = requests.Session.request
# def unsafe_request(self, *args, **kwargs):
#     kwargs['verify'] = False
#     return _old_request(self, *args, **kwargs)
# requests.Session.request = unsafe_request

# # Manual override of essential env variables
# os.environ["GOOGLE_DLP_KEY"] = r"c:\tmp\rpa-digitalisering-0eb49ea935ff.p12"
# ╔══════════════════════════════════════════════╗
# ║ 🔥 REMOVE BEFORE DEPLOYMENT (TEMP OVERRIDES) 🔥 ║
# ╚══════════════════════════════════════════════╝


load_dotenv()  # Load environment variables from .env file

RPA_CONN = RPAConnection(db_env="TEST", commit=False)
# RPA_CONN = RPAConnection(db_env="PROD", commit=False)

DB_CONN_STRING = os.getenv("DBCONNECTIONSTRINGDEV")
# DB_CONN_STRING = os.getenv("DBCONNECTIONSTRINGPROD")


async def populate_queue(workqueue: Workqueue):
    """Populate the workqueue with items to be processed."""

    logger = logging.getLogger(__name__)
    logger.info("Populating workqueue...")

    items_to_queue = retrieve_items_for_queue(logger=logger, rpa_conn=RPA_CONN, db_conn_string=DB_CONN_STRING)

    queue_references = set(str(r) for r in ats_functions.get_workqueue_items(workqueue))

    new_items: list[dict] = []

    for item in items_to_queue:
        reference = str(item.get("reference") or "")

        if reference and reference in queue_references:
            logger.info(f"Reference: {reference} already in queue. Item: {item} not added")

        else:
            new_items.append(item)

    print(f"Populating workqueue with {len(new_items)} alerts.")

    await concurrent_add(workqueue, new_items, logger)
    logger.info("Finished populating workqueue.")


async def process_workqueue(workqueue: Workqueue):
    """Process items from the workqueue."""

    logger = logging.getLogger(__name__)
    logger.info("Processing workqueue...")

    startup(logger=logger)

    error_count = 0

    while error_count < config.MAX_RETRY:
        for item in workqueue:
            try:
                with item:
                    data, reference = ats_functions.get_item_info(item)  # Item data deserialized from json as dict

                    try:
                        logger.info(f"Processing item with reference: {reference}")
                        process_item(item_data=data, item_reference=reference, rpa_conn=RPA_CONN, db_conn_string=DB_CONN_STRING)

                        completed_state = CompletedState.completed("Process completed without exceptions")  # Adjust message for specific purpose
                        item.complete(str(completed_state))

                    except BusinessError as e:
                        # A BusinessError indicates a breach of business logic or something else to be handled by business department
                        handle_error(error=e, log=logger.info, item=item, action=item.pending_user)

                        sys.exit()

                    except Exception as e:
                        # Uncaught exceptions raised to ProcessErrors
                        pe = ProcessError(str(e))
                        raise pe from e

            except ProcessError as e:
                # A ProcessError indicates a problem with the RPA process to be handled by the RPA team
                handle_error(error=e, log=logger.error, action=item.fail, item=item, send_mail=True, process_name=workqueue.name)
                error_count += 1
                reset(logger=logger)

    logger.info("Finished processing workqueue.")
    close(logger=logger)


async def finalize(workqueue: Workqueue):
    """Finalize process."""

    logger = logging.getLogger(__name__)

    logger.info("Finalizing process...")

    try:
        finalize_process()
        logger.info("Finished finalizing process.")

    except BusinessError as e:
        # A BusinessError indicates a breach of business logic or something else to be handled by business department
        handle_error(error=e, log=logger.info)

    except Exception as e:
        pe = ProcessError(str(e))
        # A ProcessError indicates a problem with the RPA process to be handled by the RPA team
        handle_error(error=pe, log=logger.error, send_mail=True, process_name=workqueue.name)

        raise pe from e


if __name__ == "__main__":

    ats = AutomationServer.from_environment()

    prod_workqueue = ats.workqueue()
    process = ats.process

    # Queue management
    if "--queue" in sys.argv:
        asyncio.run(populate_queue(prod_workqueue))

    if "--process" in sys.argv:
        # Process workqueue
        asyncio.run(process_workqueue(prod_workqueue))

    if "--finalize" in sys.argv:
        # Finalize process
        asyncio.run(finalize(prod_workqueue))

    sys.exit(0)

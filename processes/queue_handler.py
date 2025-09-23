"""Module to hande queue population"""

import asyncio
import logging

from automation_server_client import Workqueue

from mbu_dev_shared_components.database.connection import RPAConnection

from helpers import config
from helpers.get_and_store_alerts import get_alerts_past_week, update_db_with_alerts


def retrieve_items_for_queue(logger: logging.Logger, rpa_conn: RPAConnection, db_conn_string: str) -> list[dict]:
    """
    Retrieve items to be added to the workqueue.

    Returns:
        list[dict]: List of items, each item is a dictionary with at least 'reference' and 'data' keys.

    Note: This is a placeholder function. Replace with actual implementation to fetch items.
    """

    references = []

    items = []

    with rpa_conn:
        app_email = rpa_conn.get_constant("google_dlp_app_email").get("value", "")
        admin_email = rpa_conn.get_constant("google_dlp_admin_email").get("value", "")

    past_week_alerts = get_alerts_past_week(app_email=app_email, admin_email=admin_email)

    logger.info(f"Retrieved {len(past_week_alerts)} alerts from Google Alert API.")
    print(f"Retrieved {len(past_week_alerts)} alerts from Google Alert API.")

    update_db_with_alerts(past_week_alerts, db_conn_string=db_conn_string)
    print(f"Database updated with {len(past_week_alerts)} alerts.")

    for alert in past_week_alerts:
        alert_data = alert.get("data")
        if "ruleViolationInfo" not in alert_data:
            continue

        trigger_type = str(alert_data["ruleViolationInfo"]["ruleInfo"]["displayName"])
        if "cpr" not in trigger_type.lower():
            continue

        alert_id = alert.get("alertId")
        if alert_id not in (
            "9bd74b7a-463c-4594-af98-80a941a670f8",
            "EADBAFB9-64C5-4207-B9F0-4497847DDAD8",
            "391D7DEA-F2C3-4045-880F-D04AD013D6D6",
            "3C91EBEB-7262-4FBB-B70A-2B63CBEBF5C7",
        ):
            continue

        recipients = alert_data["ruleViolationInfo"]["recipients"]
        doc_link = f"https://drive.google.com/file/d/{alert_data['ruleViolationInfo']['resourceInfo']['documentId']}/view"

        references.append(alert_id)

        items.append({
            "data": {
                "alert_id": alert_id,
                "link_to_document": doc_link,
                "recipients": recipients,
                "trigger_type": trigger_type,
            },
            "reference": alert_id,
        })

    return items


async def concurrent_add(
    workqueue: Workqueue, items: list[dict], logger: logging.Logger
) -> None:
    """
    Populate the workqueue with items to be processed.
    Uses concurrency and retries with exponential backoff.

    Args:
        workqueue (Workqueue): The workqueue to populate.
        items (list[dict]): List of items to add to the queue.
        logger (logging.Logger): Logger for logging messages.

    Returns:
        None

    Raises:
        Exception: If adding an item fails after all retries.
    """
    sem = asyncio.Semaphore(config.MAX_CONCURRENCY)

    async def add_one(it: dict):
        reference = str(it.get("reference") or "")
        data = {"item": it}

        async with sem:
            for attempt in range(1, config.MAX_RETRIES + 1):
                try:
                    work_item = await asyncio.to_thread(
                        workqueue.add_item, data, reference
                    )
                    logger.info(f"Added item to queue: {work_item}")
                    return True
                except Exception as e:
                    if attempt >= config.MAX_RETRIES:
                        logger.error(
                            f"Failed to add item {reference} after {attempt} attempts: {e}"
                        )
                        return False
                    backoff = config.RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    logger.warning(
                        f"Error adding {reference} (attempt {attempt}/{config.MAX_RETRIES}). "
                        f"Retrying in {backoff:.2f}s... {e}"
                    )
                    await asyncio.sleep(backoff)

    if not items:
        logger.info("No new items to add.")
        return

    results = await asyncio.gather(*(add_one(i) for i in items))
    successes = sum(1 for r in results if r)
    failures = len(results) - successes
    logger.info(
        f"Summary: {successes} succeeded, {failures} failed out of {len(results)}"
    )

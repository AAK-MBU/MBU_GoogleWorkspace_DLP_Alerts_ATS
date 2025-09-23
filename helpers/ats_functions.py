"""Helper module to call some functionality in Automation Server using the API"""

import logging
import os

import requests
from automation_server_client import WorkItem, Workqueue
from dotenv import load_dotenv


def get_workqueue_items(workqueue: Workqueue):
    """
    Retrieve items from the specified workqueue.
    If the queue is empty, return an empty list.
    """
    load_dotenv()

    url = os.getenv("ATS_URL")
    token = os.getenv("ATS_TOKEN")

    workqueue_items = set()

    if not url or not token:
        raise EnvironmentError("ATS_URL or ATS_TOKEN is not set in the environment")

    headers = {"Authorization": f"Bearer {token}"}

    workqueue_items = set()
    page = 1
    size = 200  # max allowed

    while True:
        full_url = f"{url}/workqueues/{workqueue.id}/items?page={page}&size={size}"
        response = requests.get(full_url, headers=headers, timeout=60)
        response.raise_for_status()

        res_json = response.json().get("items", [])

        if not res_json:
            break

        for row in res_json:
            ref = row.get("reference")
            if ref:
                workqueue_items.add(ref)

        page += 1

    return workqueue_items


def get_item_info(item: WorkItem):
    """Unpack item"""
    return item.data["item"]["data"], item.data["item"]["reference"]


def init_logger():
    """Initialize the root logger with JSON formatting."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(module)s.%(funcName)s:%(lineno)d — %(message)s",
        datefmt="%H:%M:%S",
    )

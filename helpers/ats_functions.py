"""Helper module to call some functionality in Automation Server using the API"""
import os
import requests
from dotenv import load_dotenv

from automation_server_client import Workqueue, WorkItem


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

    headers = {
        "Authorization": f"Bearer {token}"
    }

    full_url = f"{url}/workqueues/{workqueue.id}/items"

    response = requests.get(full_url, headers=headers, timeout=60)

    res_json = response.json().get("items", [])
    print(res_json)

    for row in res_json:
        ref = row.get("reference")

        workqueue_items.add(ref)

    return workqueue_items


def get_item_info(item: WorkItem):
    """Unpack item"""
    return item.data["item"]["data"], item.data["item"]["reference"]

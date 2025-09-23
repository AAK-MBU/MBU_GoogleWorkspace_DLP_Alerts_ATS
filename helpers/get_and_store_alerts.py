"""
Module used for getting alert data from Google Alert API and updating the database.

This module contains functions to fetch alerts from the Google Alert API for the past week
and to update the database with these alerts. It utilizes the OrchestratorConnection to
retrieve necessary constants and credentials, and the GoogleTokenFetcher to handle
authentication with the Google API. Alerts are then inserted into the database using
a stored procedure.

Functions:
    get_alerts_one_week(oc_conn: OrchestratorConnection) -> List[Dict[str, Any]]:
        Fetches alerts from the Google Alert API for the past week.

    update_db_with_alerts(alerts: List[Dict[str, Any]], oc_conn: OrchestratorConnection) -> None:
        Updates the database with the provided alerts.
"""


import os

from typing import List, Dict, Any, Union
from datetime import date, timedelta

from mbu_dev_shared_components.google.api.auth import GoogleTokenFetcher
from mbu_dev_shared_components.google.workspace.alerts import get_alerts
from mbu_dev_shared_components.utils.db_stored_procedure_executor import execute_stored_procedure


def get_alerts_past_week(app_email: str, admin_email: str) -> List[Dict[str, Any]]:
    """
    Fetches alerts from Google Alert API for the past week.

    Args:
        oc_conn (OrchestratorConnection): An instance of OrchestratorConnection used to retrieve necessary constants.

    Returns:
        List[Dict[str, Any]]: A list of alert dictionaries retrieved from the Google Alert API.
    """

    p12_key_path = os.getenv("GOOGLE_DLP_KEY")

    scopes = ['https://www.googleapis.com/auth/apps.alerts']

    token_fetcher = GoogleTokenFetcher(p12_key_path, scopes, app_email, admin_email)
    access_token = token_fetcher.get_google_token().json()['access_token']

    date_one_week_back = date.today() - timedelta(weeks=1)
    api_filter = f'createTime >= "{date_one_week_back}T00:00:00Z"&pageSize=2000'
    alerts = get_alerts(access_token, api_filter)

    return alerts


def update_db_with_alerts(alerts: List[Dict[str, Any]]) -> Dict[str, Union[bool, str, Any]]:
    """
    Updates the database with the provided alerts.

    Args:
        alerts (List[Dict[str, Any]]): A list of alert dictionaries to be inserted into the database.
        oc_conn (OrchestratorConnection): An instance of OrchestratorConnection used to retrieve necessary constants.
    """

    db_conn = os.getenv("DBCONNECTIONSTRINGDEV")
    print(f"DB Connection string: {db_conn}")

    sp_name = "rpa.DLPGoogleAlerts_Insert"

    for alert in alerts:
        alert_data_params = {
            "customerId": ("str", alert.get("customerId")),
            "alertId": ("str", alert.get("alertId")),
            "createTime": ("datetime", alert.get("createTime")),
            "startTime": ("datetime", alert.get("startTime")),
            "endTime": ("datetime", alert.get("endTime")),
            "type": ("str", alert.get("type")),
            "source": ("str", alert.get("source")),
            "data": ("json", alert.get("data"))
        }
        update_db_result = execute_stored_procedure(db_conn, sp_name, alert_data_params)

    return update_db_result

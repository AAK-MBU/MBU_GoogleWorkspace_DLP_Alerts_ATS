"""Module to handle item processing"""
from mbu_rpa_core.exceptions import ProcessError

import os
import json
import pyodbc

from mbu_dev_shared_components.database.connection import RPAConnection

from helpers import smtp_util


def process_item(item_data: dict, item_reference: str, rpa_conn: RPAConnection, db_conn_string: str):
    """
    Function to handle item processing

    Args:
        item_data (dict): The data associated with the item.
        item_reference (str): The reference identifier for the item.

    Returns:
        None

    Raises:
        ProcessError: If a processing error occurs.
        BusinessError: If a business logic error occurs.
    """

    print(item_data, item_reference)

    try:
        with rpa_conn:
            email_sender = rpa_conn.get_constant("e-mail_noreply").get("value", "")
            smtp_port = rpa_conn.get_constant("smtp_port").get("value", "")
            smtp_adm_server = rpa_conn.get_constant("smtp_adm_server").get("value", "")

        subject = "Fil med CPR-nummer i Google Workspace skal flyttes"
        body_template = "\n<html>\n<body>\n<p>Kære {to_name},</p>\n<p>Du har uploadet en fil eller tastet et CPR-nummer i en fil i Google-miljøet. Det drejer sig om følgende fil: <a href=\"{link_to_file}\">Link til filen</a>.\nCPR-numre er i kategorien følsomme persondata, som ikke må ligge i Google-miljøet. Du skal derfor sørge for at få flyttet filen.</p>\n<p>Hvis filen indeholder CPR-nummer på en elev, skal den placeres i elevens Digitale mappe i GetOrganized.</p>\n<p>Hvis filen indeholder dit eller din families CPR-nummer, skal filen placeres i OneDrive i en mappe navngivet Privat eller lignende.</p>\n<p><a href=\"https://intranet.aarhuskommune.dk/documents/128713\">Hvis du vil vide mere kan du læse om dette her på AarhusIntra</a></p>\n</body>\n</html>"

        alert_id = item_reference
        print(f"alert id: {alert_id}")

        with pyodbc.connect(db_conn_string) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    [alertId],
                    [recipients],
                    [link],
                    [isNotified],
                    [azident],
                    [navn],
                    [matched_recipient_emails]
                FROM
                    [RPA].[rpa].[DLPGoogleAlertsView_DADJ_TEST]
                WHERE
                    isNotified = 0
                    AND triggerType = 'CPR-Number'
                    AND matched_recipient_emails IS NOT NULL
                    AND alertId = ?
                """,
                alert_id
            )

            row = cursor.fetchone()

            if row:
                names = row.navn.split('; ')
                emails = row.matched_recipient_emails.split('; ')

                for name, email in zip(names, emails):
                    body = body_template.format(to_name=name, link_to_file=row.link)

                    smtp_util.send_email(
                        # receiver=email,
                        receiver="dadj@aarhus.dk",
                        sender=email_sender,
                        subject=subject,
                        body=body,
                        html_body=True,
                        smtp_server=smtp_adm_server,
                        smtp_port=smtp_port
                    )

                cursor.execute("EXEC [rpa].[DLPGoogleAlerts_Insert] @alertId = ?, @isNotified = ?", row.alertId, 1)
                conn.commit()

            else:
                raise ProcessError("No matching row found!")

    except pyodbc.Error as e:
        print(f"Database error: {str(e)}")

    except json.JSONDecodeError as e:
        print(f"JSON decode error: {str(e)}")

    except ValueError as e:
        print(f"Value error: {str(e)}")













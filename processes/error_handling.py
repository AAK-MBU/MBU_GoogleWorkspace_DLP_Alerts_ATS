"""Module for handling errors"""

import json
import smtplib
from email.message import EmailMessage
import base64
from io import BytesIO

from PIL import ImageGrab

from mbu_dev_shared_components.database.connection import RPAConnection
from mbu_rpa_core.exceptions import ProcessError, BusinessError
from automation_server_client import WorkItem


def handle_error(
        error: ProcessError | BusinessError,
        log,
        item: WorkItem | None = None,
        action = None,
        send_mail = False,
        add_screenshot = True,
        process_name = None,
):
    """Function to log error"""
    error_json = json.dumps(error.__dictinfo__())
    log_msg = f"Error: {error}"
    if item:
        log_msg = f"{repr(error)} raised for item: {item}. " + log_msg
        action(error_json)
    log(log_msg)
    if send_mail:
        send_error_email(error=error, add_screenshot=add_screenshot, process_name=process_name)


def send_error_email(error: ProcessError | BusinessError, add_screenshot: bool = False, process_name: str | None = None):
    """Send email to defined recipient with error information"""
    rpa_conn = RPAConnection(db_env="PROD", commit=False)
    with rpa_conn:
        error_email = rpa_conn.get_constant("Error Email")["value"]
        error_sender = rpa_conn.get_constant("Email Friend")["value"]  # Find in database...
        smtp_server = rpa_conn.get_constant("smtp_server")["value"]
        smtp_port = rpa_conn.get_constant("smtp_port")["value"]
    # Create message
    msg = EmailMessage()
    msg['to'] = error_email
    msg['from'] = error_sender
    msg['subject'] = (
        "Error screenshot"
        + f": {process_name}" if process_name else ""
    )

    if add_screenshot:
        screenshot = grab_screenshot()

    # Create an HTML message with the exception and screenshot
    error_dict = error.__dictinfo__()
    html_message = (
        f"""
        <html>
            <body>
                <p>Error type: {error_dict['type']}</p>
                <p>Error message: {error_dict['message']}</p>
                <p>{error_dict['traceback']}</p>
        """ +
        f'      <img src="data:image/png;base64,{screenshot}" alt="Screenshot">' if add_screenshot else '' +
        """
            </body>
        </html>
        """
    )

    msg.set_content("Please enable HTML to view this message.")
    msg.add_alternative(html_message, subtype='html')

    # Send message
    with smtplib.SMTP(smtp_server, smtp_port) as smtp:
        smtp.starttls()
        smtp.send_message(msg)


def grab_screenshot():
    """Grabs screenshot"""
    # Take screenshot and convert to base64
    screenshot = ImageGrab.grab()
    buffer = BytesIO()
    screenshot.save(buffer, format='PNG')
    screenshot_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')

    return screenshot_base64

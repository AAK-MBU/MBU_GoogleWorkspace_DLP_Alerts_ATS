"""Module to handle item processing"""
# from mbu_rpa_core.exceptions import ProcessError, BusinessError


def process_item(item_data: dict, item_reference: str):
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

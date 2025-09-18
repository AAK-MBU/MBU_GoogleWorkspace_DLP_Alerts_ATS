"""Module for handling application startup, and close"""


import logging


def startup(logger: logging.Logger):
    """Function for starting applications"""
    logger.info("Starting applications...")


def soft_close(logger: logging.Logger):
    """Function for closing applications softly"""
    logger.info("Closing applications softly...")


def hard_close(logger: logging.Logger):
    """Function for closing applications hard"""
    logger.info("Closing applications hard...")


def close(logger: logging.Logger):
    """Function for closing applications softly or hardly if necessary"""
    try:
        soft_close(logger)
    except Exception:
        hard_close(logger)


def reset(logger: logging.Logger):
    """Function for resetting application"""
    close(logger)
    startup(logger)
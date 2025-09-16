"""Module for handling application startup, and close"""


def startup():
    return


def soft_close():
    return


def hard_close():
    return


def close():
    try:
        soft_close()
    except Exception:
        hard_close()


def reset():
    close()
    startup()

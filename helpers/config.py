"""Module for general configurations of the process"""

# ----------------------
# Workqueue settings
# ----------------------
MAX_RETRY = 10

# ----------------------
# Queue population settings
# ----------------------
MAX_CONCURRENCY = 10  # tune based on backend capacity
MAX_RETRIES = 3  # failure retries per item
RETRY_BASE_DELAY = 0.5  # seconds

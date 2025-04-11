from datetime import datetime

from . import config

PATH_TO_LOGFILE = ''

def log_progress(msg, tee_enabled=True):
    """Write event log with timestamp to file specified.

    Keyword Arguments:
    msg -- string message to log
    tee -- optional boolean to enable mirroring of log message to stdout
        Default: True

    Return: None
    """
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp_str = now.strftime(timestamp_format)

    log_str = timestamp_str + ": " + msg + "\n"

    if tee_enabled:
        print(log_str, end='')

    with open(config.PATH_TO_LOGFILE, "a") as f:
        f.write(log_str)

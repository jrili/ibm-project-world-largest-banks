from datetime import datetime

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

    with open(PATH_TO_LOGFILE, "a") as f:
        f.write(log_str)

def set_path_to_logfile(path_to_logfile):
    global PATH_TO_LOGFILE
    PATH_TO_LOGFILE = path_to_logfile
    log_progress(f"Log file set to '{PATH_TO_LOGFILE}'.")

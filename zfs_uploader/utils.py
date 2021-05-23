from datetime import datetime

from zfs_uploader import DATETIME_FORMAT


def get_date_time():
    """ Get datetime object in correct format.

    Returns
    -------
    datetime

    """
    return datetime.now().strftime(DATETIME_FORMAT)

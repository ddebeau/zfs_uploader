from datetime import datetime

from zfs_uploader import DATETIME_FORMAT


def get_date_time():
    return datetime.now().strftime(DATETIME_FORMAT)

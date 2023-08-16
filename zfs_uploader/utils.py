from datetime import datetime

from zfs_uploader import DATETIME_FORMAT


def get_date_time():
    """ Get datetime object in correct format.

    Returns
    -------
    datetime

    """
    return datetime.now().strftime(DATETIME_FORMAT)


def derive_s3_key(object_name, filesystem, prefix=None):
    """
    Derive the s3 key for the object.

    Parameters
    ----------
    object_name : str
      The object name. Such as backup.db or the full/inc snapshot name.
    filesystem : str
      The ZFS filesystem.
    prefix : str, optional
      The s3 prefix to prepend to the key.

    Returns
    -------
    string

    """
    s3_key = f'{filesystem}/{object_name}'
    if prefix is not None:
        s3_key = f'{prefix}/{s3_key}'
    return s3_key

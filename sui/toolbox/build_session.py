"""Split user's log data into multiple sessions
Date: 02/Mar/2021
Author: Li Tang
"""
from typing import Union

__author__ = ['Li Tang']
__copyright__ = 'Li Tang'
__credits__ = ['Li Tang']
__license__ = 'MIT'
__version__ = '0.1.11'
__maintainer__ = ['Li Tang']
__email__ = 'litang1025@gmail.com'
__status__ = 'Production'


def build_session(data: Union[list, tuple], user_idx: int, item_idx: int, timestamp_idx: int, session_gap: int):
    """Function to split data into multiple sessions

    Args:
        data: user's log data sorted input data consists of lists or tuples, including user id, item id, and timestamp
        user_idx: index of the user id in each user's log data
        item_idx: index of the item id in each user's log data
        timestamp_idx: index of the timestamp in each user's log data
        session_gap: the threshold to split user's activities into multiple sessions based the gap of adjacent timestamps

    Returns:
        a list sorted by target dimension with length no greater than k

    Examples:
        >>> log_data = (('user_867as8e', 'v9d8cv8272lk', 1614652639000), ('user_867as8e', 'v8d4ln9834kj', 1614653499000), ('user_868yu82', 'n0s3mn43k4n3', 1614653646000), ('user_868yu82', 'v987d3n5l89n', 1614653702246))
        >>> build_session(data=log_data, user_idx=0, item_idx=1, timestamp_idx=2, session_gap=600000)
        [('user_867as8e', ['v9d8cv8272lk']), ('user_867as8e', ['v8d4ln9834kj']), ('user_868yu82', ['n0s3mn43k4n3', 'v987d3n5l89n'])]

    """
    is_first_log = True
    result = []
    user_id = ''
    current_ts = 0
    session = []
    for log in data:
        if is_first_log:
            user_id = log[user_idx]
            current_ts = log[timestamp_idx]
            is_first_log = False

        if user_id == log[user_idx] and log[timestamp_idx] - current_ts <= session_gap:
            session.append(log[item_idx])
        else:
            result.append((user_id, session))
            session = [log[item_idx]]
            user_id = log[user_idx]
            current_ts = log[timestamp_idx]
    result.append((user_id, session))

    return result

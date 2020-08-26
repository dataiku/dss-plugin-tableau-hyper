"""
Handle write of temporary file with respect to DSS MUS instance

"""

import os
import pwd

CACHE_RELATIVE_DIR = '.cache/dss/plugins/tableau-hyper-export'


def get_cache_location_from_user_config():
    """
    Get the cache location for temporary files creation, the target cache location will be associated
    with the user id that is launching the export
    :return: absolute location of cache
    """
    home_dir = pwd.getpwuid(os.getuid()).pw_dir
    cache_absolute_dir = os.path.join(home_dir, CACHE_RELATIVE_DIR)
    if not os.path.exists(cache_absolute_dir):
        os.makedirs(cache_absolute_dir)
    return cache_absolute_dir

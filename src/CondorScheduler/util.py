import os, os.path
import math
import itertools as it

__all__ = [
    "partition",
    "grouper",
    "make_sure_path_exists"
]

def partition(iterable, n, fillvalue=None):
    """
    Collect data into fixed-length chunks or blocks

    >>> partition('ABCDEFG', 3, 'x')
    'ABC' 'DEF' 'Gxx'
    """
    args = [iter(iterable)] * n
    return it.zip_longest(*args, fillvalue=fillvalue)


def grouper(iterable, min_size, max_groups=None):
    group_size = min_size \
        if max_groups is None \
        else int(math.ceil(len(iterable) / max_groups))

    return partition(iterable, group_size)


def make_sure_path_exists(path):
    """
    Creates the supplied <path> if it does not exist.
    Raises OSError if the <path> cannot be created.

    Parameters
    ----------

    path : str
        Path to create.

    Returns
    -------

    None
    """
    try:
        os.makedirs(path)
    except OSError:
        if not os.path.isdir(path):
            raise

from os import path
from .util import grouper

__all__ = [
    "make_files"
]

def make_files(iterable, dir, min_size, max_groups=None):
    """
    Takes an iterable, separates it into equally sized groups
    (padding the last group with None's), and writes each group to a file,
    returning the list of file names.

    Parameters
    ----------

    iterable : iterable
        Iterable to split into files.
    dir : str
        Directory to write files to.
    min_size : int
        Minimum number of items to put in each group.
    max_groups : int, optional
        Maximum number of groups to create.
    """
    return [
        make_file(i, group, dir)
        for i, group
        in enumerate(grouper(iterable, min_size, max_groups))
    ]


def make_file(i, lines, dir):
    """
    Create a file named "<dir>/<i>.input", and write each line in <lines> to it.

    Parameters
    ----------

    i : int
        File's index.
    lines : iterable
        Iterable containing strings to write to file.
    dir : str
        Directory to create file in

    Returns
    -------

    fname : str
        Path to created file.
    """
    fname = path.join(dir, "{}.input".format(i))

    with open(fname, "w") as f:
        f.writelines([line for line in lines if line is not None])
        return f.name

#!/usr/bin/env python3
from sparse_file import open_sparse
from sys import argv
from pathlib import Path

FS_PAGESIZE=4096
NULLPAGE = FS_PAGESIZE * b'\x00'


def nullblockstreaks(filepath: str):
    with open(filepath, 'rb') as thefile:
        last_start = None
        cur_streak = 0
        for ix, block in enumerate(iter(lambda: thefile.read(FS_PAGESIZE), b'')):
            if block == NULLPAGE:
                if last_start is None:
                    last_start = ix
                cur_streak += 1
            else:
                if last_start is not None:
                    yield (last_start, cur_streak)
                    last_start = None
                    cur_streak = 0
        else:
            if last_start:
                yield (last_start, cur_streak)


def sparseify(filepath: str):
    with open_sparse(filepath, 'ab') as thefile:
        thefile.seek(0)
        for block_ix, no_blocks in nullblockstreaks(filepath):
            thefile.hole(block_ix * FS_PAGESIZE, no_blocks * FS_PAGESIZE)


def main():
    try:
        if not Path(argv[1]).is_file():
            exit(f'Not a file: "{argv[1]}"')
    except IndexError:
        exit('Usage: sparseify.py /path/to/file')


if __name__ == '__main__':
    main()

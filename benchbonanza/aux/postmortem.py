#!/usr/bin/env python3
from os import execvp
from sys import argv
from re import compile as rexcompile, ASCII

NICE_INVOCATION_ID = rexcompile(r'\A[a-z0-9]{32}\Z', ASCII)

def journal_for_invocation_id(invocation_id):
    argv = [
        '--output=cat',
        '--output-fields=MESSAGE',
        '_TRANSPORT=stdout',
        f'_SYSTEMD_INVOCATION_ID={invocation_id}',
    ]
    execvp('journalctl', argv)


def main():
    try:
        invocation_id = argv[1].replace('-', '')
    except IndexError:
        exit(f'Usage: {argv[0]} invocation-id')
    if not NICE_INVOCATION_ID.match(invocation_id):
        exit(f'Fatal: "{argv[1]}" does not look like a systemd invocation ID')
    journal_for_invocation_id(invocation_id)


if __name__ == '__main__':
    main()

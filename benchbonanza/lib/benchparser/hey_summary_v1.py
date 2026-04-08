#!/bin/env python3
from sys import argv
from pathlib import Path
from re import compile, ASCII, MULTILINE

def rexcompile(exp):
    return compile(exp, ASCII | MULTILINE)

extracts = dict(
    walltime = rexcompile(r'^  Total:\s+(?P<thenumber>\d+(\.\d+)?) secs$'),
    latency_percentile = rexcompile(r'^  (?P<percentile>10|25|50|75|90|95|99)% in (?P<thenumber>\d+(\.\d+)?) secs$'),
    status = rexcompile(r'^  \[(?P<httpstatus>\d{3})\]\s+(?P<thenumber>\d+)\s+responses$'),
    error = rexcompile(r'^  \[(?P<thenumber>\d+)\]\s+\D+.*$'),
)

def extract(buf):
    def num_from_match(match, mult=1):
        return int(round(mult * float(match.groupdict()['thenumber'])))

    def firstmatch(metric):
        return next(extracts[metric].finditer(buf), None)

    if m := firstmatch('walltime'):
        yield ('walltime', num_from_match(m, 1000))
    if m := firstmatch('error'):
        yield 'error', num_from_match(m)
    for m in extracts['latency_percentile'].finditer(buf):
        yield (f'latency.{m.groupdict()['percentile']}', num_from_match(m, mult=1_000))
    for m in extracts['status'].finditer(buf):
        yield (f'httpstatus.{m.groupdict()['httpstatus']}', num_from_match(m))


if __name__ == '__main__':
    for thing in extract(Path(argv[1]).read_text()):
        print(*thing)

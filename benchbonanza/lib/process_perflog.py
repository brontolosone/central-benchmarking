#!/usr/bin/env python3

from re import compile as rexcompile
from sys import stdin

re_logline = rexcompile(
    r"^(?P<time>\d+)\t(?P<service>[^/]+)/(?P<category>[^:]+):(?P<payload>.*)$"
)


class ParseError(Exception):
    pass


def read_log(fh):
    # it starts with the monotonic clock offset
    yield float(fh.readline().strip())
    for line in fh:
        stripped = line.strip()
        if isinstance(stripped, bytes):
            stripped = stripped.decode("utf-8")
        if stripped:  # tolerate empty lines
            try:
                time, service, category, payload = re_logline.match(stripped).groups()
                time = int(time)
                if category.endswith(".pressure"):
                    # These are multival-per-line. Ex: some avg10=0.00 avg60=0.00 avg300=0.00 total=221661
                    # Change into one val per line.
                    subcategory, *metrics = payload.split()
                    for m in metrics:
                        submetric, value = m.split("=", maxsplit=1)
                        value_inted = int(
                            100 * float(value)
                        )  # a percentage re-expressed as "ten-thousands", so that it's an int for storage. Divide by 100.0 to get the percentage again.
                        yield (
                            time,
                            service,
                            f"{category}.{subcategory}.{submetric}",
                            value_inted,
                        )
                elif category.endswith(".peak"):
                    # bare value
                    yield (time, service, f"{category}", int(payload))
                else:
                    label, value = payload.rsplit(" ", maxsplit=1)
                    yield (time, service, f"{category}.{label}", int(value))
            except ValueError as err:
                raise ParseError(f"Parsing failed: {line}") from err


if __name__ == "__main__":
    measurements = read_log(stdin)
    print(next(measurements))
    for measurement in measurements:
        print("\t".join(map(str, measurement)))

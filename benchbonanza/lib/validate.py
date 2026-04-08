#!/usr/bin/env python3
"""
Validators for config file formats
"""
from tomllib import load as tomlload, TOMLDecodeError
from json import load as jsonload
from jsonschema import validate, ValidationError as JsonSchemaValidationError
from pathlib import Path
from sys import argv
from os import environ
from itertools import chain
from collections import Counter

GIT_REPO_PREFIX_ALLOWED=set(environ.get('BB_GIT_REPO_PREFIXES_ALLOWED', 'https://').split())


class ValidationError(Exception):
    pass


class UniqueConstraintError(ValidationError):
    pass


class DisallowedUrlError(ValidationError):
    pass


def validate_against(schemafile: str, conf_file: Path):
    schema, conf = {}, {}
    with (Path(__file__).parent.parent / 'var' / schemafile).open('rb') as fp:
        schema = jsonload(fp)
    with conf_file.open('rb') as fp:
        conf = tomlload(fp)
    validate(conf, schema)
    return conf


def validate_repoconf(confpath: Path, url_prefix_allowed=GIT_REPO_PREFIX_ALLOWED):
    try:
        conf = validate_against('repos.schema.json', confpath)
    except TOMLDecodeError as e:
        raise ValidationError from e
    urls = Counter(v['url'] for moniker, v in conf['repos'].items())
    urlfreqs = urls.most_common(1)
    if urlfreqs and urlfreqs[0][1] > 1:
        raise UniqueConstraintError('URLs must be distinct. Non-distinct: %s' % urlfreqs[0][0])
    if url_prefix_allowed:
        for url in urls:
            if not any((url.startswith(prefix) for prefix in url_prefix_allowed)):
                raise DisallowedUrlError(f'URL "{url}" does not start with any of {url_prefix_allowed}')
    return conf


def validate_expconf(confpath: Path):
    try:
        conf = validate_against('exp.schema.json', confpath)
    except (TOMLDecodeError, JsonSchemaValidationError) as e:
        raise ValidationError from e
    freqs = Counter(chain.from_iterable((entry['name'] for entry in conf.get(section, [])) for section in ("setup", "trackedservice", "testload"))).most_common(1)
    if freqs and freqs[0][1] > 1:
        raise UniqueConstraintError('Names for items must be distinct. Non-distinct: %s' % freqs[0][0])
    return conf


def main():
    actionmap = {
        "validate-repoconf" : validate_repoconf,
        "validate-expconf" : validate_expconf,
    }
    usage = f'Usage: {argv[0]} ({' | '.join(actionmap)}) FILENAME'

    if not len(argv) == 3:
        exit(usage)
    try:
        action = actionmap[argv[1]]
    except KeyError:
        exit(usage)
    thefilepath = Path(argv[2])
    if not thefilepath.is_file():
        exit(f'Not a file: {thefilepath}')
    action(thefilepath)


if __name__=='__main__':
    main()

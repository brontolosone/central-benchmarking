from binascii import unhexlify, hexlify
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC as dtUTC
from datetime import datetime as dt
from functools import partial
from hashlib import md5
from os import environ
from pathlib import Path
from shutil import rmtree
from subprocess import DEVNULL, PIPE, Popen, check_call


from tomllib import load as tomlload

from . import constants as const
from .validate import ValidationError, validate_expconf, validate_repoconf


def utcnow():
    return dt.now(dtUTC)


def shorthash(thing):
    thang = thing.encode("utf-8") if isinstance(thing, str) else thing
    return md5(thang).hexdigest()[: const.SHORTHASH_LENGTH]


class GitState:
    def __init__(self, statedir: Path):
        self.statedir = statedir
        self.configrepo_path = self.statedir / const.CONFIGREPO_DIR
        self.repoconf_path = self.configrepo_path / const.REPOCONF_FILENAME
        self.exp_path = self.configrepo_path / const.EXPDEF_DIR
        self.coderepos_path = self.statedir / const.REPOS_DIR
        self.stderr = None if environ.get('BB_VERBOSITY') == '1' else DEVNULL
        self.check_call_hushed = partial(
            check_call, stdin=DEVNULL, stdout=DEVNULL, stderr=self.stderr, env={**environ, 'GIT_TERMINAL_PROMPT':'false'}
        )

    def repopath(self, conf):
        return self.coderepos_path / shorthash(conf["url"])

    def export_repo(self, src, at_commit):
        return Popen(
            ['git', 'archive', '--format=tar', at_commit],
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=self.stderr,
            cwd=src,
        )

    def git_clone(self, dest, repo_url):
        # Note: Deliberately prevents symlinks in the repo from becoming symlinks in the FS
        dest.mkdir(exist_ok=False)
        self.check_call_hushed(["git", "init", "--quiet"], cwd=dest)
        self.check_call_hushed(
            ["git", "config", "core.symlinks", "false"], cwd=dest
        )
        self.check_call_hushed(
            ["git", "remote", "add", "origin", repo_url], cwd=dest
        )

    def update_clone(self, dest, repo_url):
        try:
            if not dest.is_dir():
                self.git_clone(dest, repo_url)
            self.check_call_hushed(
                ["git", "remote", "update", "--prune"],
                cwd=dest,
                timeout=const.GIT_UPDATE_TIMEOUT,
            )
            return (True, utcnow())
        except Exception as e:
            return (False, utcnow(), e)

    def git_mirror(self, dest, repo_url):
        dest.mkdir()
        self.check_call_hushed(
            ["git", "clone", "--quiet", "--mirror", repo_url, "."],
            cwd=dest,
            timeout=const.GIT_MIRROR_TIMEOUT,
        )

    def update_mirror(self, dest, repo_url):
        try:
            if not dest.is_dir():
                self.git_mirror(dest, repo_url)
            self.check_call_hushed(
                ["git", "remote", "update", "--prune"],
                cwd=dest,
                timeout=const.GIT_UPDATE_TIMEOUT,
            )
            return (True, utcnow())
        except Exception as e:
            return (False, utcnow(), e)

    def popen_iter(self, cwd, argv):
        stdio = dict(stdin=DEVNULL, stdout=PIPE, stderr=self.stderr)
        with Popen(argv, cwd=cwd, **stdio) as proc:
            yield from iter(proc.stdout.readline, b"")

    def get_commits_and_tags(self, repopath, commitfilter=None):
        # yields commit, timestamp, {tags}
        filter_in = (lambda a: True) if commitfilter is None else commitfilter.__contains__
        sans_tagprefix = slice(5, None)
        logformat = r"%H%x09%ct%x09%D%x09"
        for line in self.popen_iter(
            repopath,
            [
                "git",
                "log",
                "--all",
                f"--format={logformat}",
                "--decorate-refs=refs/tags",
            ],
        ):
            commit, ts_raw, tags_raw, *_ = line.split(b"\t", maxsplit=3)
            if filter_in(commit):
                tags = {
                    f"{rawtag[sans_tagprefix].decode('utf8')}"
                    for rawtag in tags_raw.split(b", ")
                    if rawtag
                }
                yield (unhexlify(commit), int(ts_raw), tags)

    def get_noterefs(self, repopath, nsfilter=None):
        notens_slice = slice(6, -1)
        filter_in = (lambda a: True) if nsfilter is None else nsfilter.__contains__
        for line in self.popen_iter(
            repopath,
            [
                "git",
                "for-each-ref",
                "refs/notes",
                "--format=%(refname:short)",
            ],
        ):
            notens = line[notens_slice]
            if filter_in(notens):
                yield notens

    def get_commits_for_noteref(self, repopath, noteref):
        commit_slice = slice(41, 81)
        for line in self.popen_iter(
            repopath,
            [
                b"git",
                b"notes",
                b"--ref",
                noteref
            ]
        ):
            yield unhexlify(line[commit_slice])

    def get_experiments(self, with_machines=False):
        for p in self.exp_path.glob("*/*.toml", recurse_symlinks=False):
            if p.is_file() and not p.is_symlink():
                if const.RE_LTREELABEL.match(
                    p.parent.name
                ) and const.RE_LTREELABEL.match(p.stem):
                    if with_machines:
                        try:
                            machines = set(filter(const.RE_LTREEPATH.match, validate_expconf(p)["meta"]["machines"]))
                            yield ((p.parent.name, p.stem, machines), p)
                        except ValidationError:
                            pass
                    else:
                        yield ((p.parent.name, p.stem), p)

    def get_expconf(self, experiment, variant, validate=True):
        exp_path = self.exp_path / experiment / f'{variant}.toml'
        return validate_expconf(exp_path)

    def get_repoconf(self, validate=False):
        if validate:
            return validate_repoconf(self.repoconf_path)
        sane_confs = {}
        with self.repoconf_path.open("rb") as fp:
            maybe_config = tomlload(fp)
            reposconf = maybe_config.get("repos", {})
            for moniker, conf in reposconf.items():
                if const.RE_LTREELABEL.match(moniker) and isinstance(conf, dict):
                    if url := conf.get("url"):
                        if isinstance(url, str):
                            # we have enough to work with!
                            sane_conf = dict(url=url)
                            if conf.get("paused") is True:
                                sane_conf["paused"] = True
                            sane_confs[moniker] = sane_conf
        return sane_confs

    def sync_configrepo(self):
        try:
            self.update_clone(
                self.configrepo_path, environ["CONFIGREPO_GIT_URL"]
            )
            self.check_call_hushed(
                ["git", "reset", "--quiet", "--hard", "refs/remotes/origin/master"],
                cwd=self.configrepo_path,
            )
            return (True, utcnow())
        except Exception as e:
            return (False, utcnow(), e)

    def sync_coderepos(self, repoconf):
        self.coderepos_path.mkdir(exist_ok=True)
        sync_outcomes = {}
        with ThreadPoolExecutor(
            min(len(repoconf), const.MAX_SYNCTASKS),
            thread_name_prefix="benchbonanza-gitsync-",
        ) as execpool:
            task_x_moniker = {}
            for repomoniker, conf in repoconf.items():
                if repomoniker == const.CONFIGREPO_RESERVED_MONIKER:
                    continue
                if conf.get("paused") is not True:
                    task_x_moniker[
                        execpool.submit(
                            self.update_mirror, self.repopath(conf), conf["url"]
                        )
                    ] = repomoniker
                else:
                    sync_outcomes[repomoniker] = (None, utcnow())
            for future in as_completed(task_x_moniker):
                repomoniker = task_x_moniker[future]
                sync_outcomes[repomoniker] = future.result()
                del task_x_moniker[future]
        return sync_outcomes

    def sync(self):
        sync_outcomes = {}
        sync_outcomes[const.CONFIGREPO_RESERVED_MONIKER] = self.sync_configrepo()
        self.garbagecollect_repos()
        repoconf = self.get_repoconf()
        sync_outcomes.update(self.sync_coderepos(repoconf))
        return sync_outcomes

    def garbagecollect_repos(self):
        # We really only want to GC if the repoconf is valid,
        # we don't want to go by the best-effort parse in this case.
        dir_exclude = {}
        try:
            repoconf = self.get_repoconf(validate=True)
        except:  # noqa E722
            return False
        dir_exclude = {shorthash(conf["url"]) for conf in repoconf.get('repos', {}).values()}
        for junkdir in filter(
            lambda p: p.name not in dir_exclude and p.is_dir,
            self.coderepos_path.glob(const.SHORTHASH_LENGTH * "?"),
        ):
            rmtree(junkdir)
        return True

    def get_state(self):
        repoconf = self.get_repoconf()
        experiments = {".".join(exp): path for exp, path in self.get_experiments()}
        noteref_experiment = {
            f'bb@{exp}'.encode('utf-8'): exp for exp in experiments
        }

        repos_to_process = {repomoniker: self.repopath(conf) for repomoniker,conf in repoconf.items() if self.repopath(conf).is_dir()}  # dirless repos were probably added in a paused state and thus were not yet mirrored

        # Pass one: Get all experiment-note-tagged commits from all repos
        repo_exp_x_commits = defaultdict(lambda: defaultdict(set))
        commits_seen = set()

        for repomoniker, repopath in repos_to_process.items():
            note_namespaces_present_in_repo = set(self.get_noterefs(repopath, nsfilter=noteref_experiment))
            for noteref in note_namespaces_present_in_repo:
                commits = set(self.get_commits_for_noteref(repopath, noteref))
                repo_exp_x_commits[repomoniker][noteref_experiment[noteref]] |= commits
                commits_seen |= commits

        # Pass two: Look up all tags of those commits of interest
        all_notereffed_commits = frozenset(map(hexlify, commits_seen))
        commit_x_ts = dict()
        repocommit_x_tags = defaultdict(lambda: defaultdict(set))

        for repomoniker, repopath in repos_to_process.items():
            for commit, ts, tags in self.get_commits_and_tags(repopath, commitfilter=all_notereffed_commits):
                commit_x_ts[commit] = ts
                if tags:
                    repocommit_x_tags[repomoniker][commit] = tags

        return dict(
            commits = commit_x_ts,
            applications = repo_exp_x_commits,
            repocommit_tags = repocommit_x_tags,
        )

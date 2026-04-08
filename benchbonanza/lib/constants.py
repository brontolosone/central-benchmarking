from re import compile as rexcompile
from enum import Enum

CONFIGREPO_DIR = 'configrepo'
CONFIGREPO_RESERVED_MONIKER = 'CONFIGREPO'
EXPRUN_DIR = 'runs'
EXPDEF_DIR = 'experiments'
REPOS_DIR = 'repos'
REPOCONF_FILENAME = 'repos.toml'
CRASHLOG = 'crash.log'

GIT_MIRROR_TIMEOUT = 600  # can be a lot
GIT_UPDATE_TIMEOUT = 60
MAX_SYNCTASKS = 16  # max concurrent git sync operations
SHORTHASH_LENGTH = 16

LTREELABEL = r'[a-zA-Z0-9_-]+'
RE_PERIOD = r'\.'
RE_LTREELABEL = rexcompile(f'\\A{LTREELABEL}\\z')
RE_LTREEPATH = rexcompile(f'\\A{LTREELABEL}({RE_PERIOD}{LTREELABEL})*\\z')

VM_TIMEOUT_MAX = 1200

class Exitcode(Enum):
    DB_DISCONNECT = 101
    DB_RUN_ALREADY_EXISTS = 102
    DB_RUN_LOCKED = 103
    DB_RUN_INVALID_FOREIGN_KEY = 104
    VM_TIMEOUT_EXPIRED = 105
    UNKNOWN_MACHINE = 106
    SETUP_ERROR = 107
    VM_PROCESS_ERROR = 108
    DB_RUN_APPLICATION_NONEXISTENT = 109

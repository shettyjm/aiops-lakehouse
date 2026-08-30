"""Config loading for aiops components.

Single source of truth for reading config.ini. Never hardcode endpoints or
credentials elsewhere — go through load_config(). Environment variables of the
form AIOPS_<SECTION>_<KEY> override the file (handy for CI and for keeping
secrets out of the ini), e.g. AIOPS_MINIO_SECRET_KEY.
"""

from __future__ import annotations

import configparser
import os
from pathlib import Path

# Repo root is the parent of the aiops/ package directory.
REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "config.ini"
EXAMPLE_CONFIG_PATH = REPO_ROOT / "config.ini.example"

_ENV_PREFIX = "AIOPS"


def _apply_env_overrides(parser: configparser.ConfigParser) -> None:
    """Override any value from AIOPS_<SECTION>_<KEY> environment variables."""
    for section in parser.sections():
        for key in parser.options(section):
            env_name = f"{_ENV_PREFIX}_{section.upper()}_{key.upper()}"
            if env_name in os.environ:
                parser.set(section, key, os.environ[env_name])


def load_config(path: str | os.PathLike | None = None) -> configparser.ConfigParser:
    """Load config.ini into a ConfigParser, applying env overrides.

    Falls back to config.ini.example if config.ini is absent, so `--source local`
    workflows and tests run with no setup. Raises FileNotFoundError only if
    neither file exists.
    """
    parser = configparser.ConfigParser()
    if path is not None:
        chosen = Path(path)
    elif DEFAULT_CONFIG_PATH.exists():
        chosen = DEFAULT_CONFIG_PATH
    else:
        chosen = EXAMPLE_CONFIG_PATH

    if not chosen.exists():
        raise FileNotFoundError(
            f"No config found at {chosen}. Copy config.ini.example to config.ini."
        )

    parser.read(chosen)
    _apply_env_overrides(parser)
    return parser


def get_bool(parser: configparser.ConfigParser, section: str, key: str,
             fallback: bool = False) -> bool:
    """Read a boolean value tolerating true/false/yes/no/1/0."""
    if not parser.has_option(section, key):
        return fallback
    return parser.getboolean(section, key)

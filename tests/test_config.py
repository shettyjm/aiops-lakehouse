"""Tests for the config loader — the one piece of M0 with real logic."""

import textwrap

import pytest

from aiops.config import EXAMPLE_CONFIG_PATH, get_bool, load_config


def test_example_config_has_required_sections_and_keys():
    """config.ini.example must carry every section/key the components rely on."""
    cfg = load_config(EXAMPLE_CONFIG_PATH)
    expected = {
        "minio": ["endpoint", "access_key", "secret_key", "secure", "raw_bucket"],
        "iceberg": ["uri", "warehouse", "namespace", "s3_endpoint",
                    "access_key", "secret_key"],
        "model": ["backend", "base_url", "model_name"],
        "detect": ["heap_limit_mb", "io_z_threshold", "retrans_pct", "latency_x"],
    }
    for section, keys in expected.items():
        assert cfg.has_section(section), f"missing [{section}]"
        for key in keys:
            assert cfg.has_option(section, key), f"missing {section}.{key}"


def test_example_defaults_match_contract():
    cfg = load_config(EXAMPLE_CONFIG_PATH)
    assert cfg.get("minio", "raw_bucket") == "telemetry-raw"
    assert cfg.get("iceberg", "warehouse") == "telemetry"
    assert cfg.get("iceberg", "namespace") == "observability"
    assert cfg.getint("detect", "heap_limit_mb") == 4096
    assert cfg.getint("detect", "io_z_threshold") == 4


def test_env_override(tmp_path, monkeypatch):
    ini = tmp_path / "config.ini"
    ini.write_text(textwrap.dedent("""
        [minio]
        endpoint = original:9000
        secret_key = file-secret
    """))
    monkeypatch.setenv("AIOPS_MINIO_SECRET_KEY", "env-secret")
    cfg = load_config(ini)
    assert cfg.get("minio", "secret_key") == "env-secret"
    assert cfg.get("minio", "endpoint") == "original:9000"


def test_get_bool_variants(tmp_path):
    ini = tmp_path / "config.ini"
    ini.write_text(textwrap.dedent("""
        [minio]
        secure = false
        other = yes
    """))
    cfg = load_config(ini)
    assert get_bool(cfg, "minio", "secure") is False
    assert get_bool(cfg, "minio", "other") is True
    assert get_bool(cfg, "minio", "missing", fallback=True) is True


def test_missing_config_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "does-not-exist.ini")

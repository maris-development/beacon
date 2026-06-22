"""Tests for connection config resolution and auth handling."""

from __future__ import annotations

from beacon_cli.config import DEFAULT_URL, ClientConfig


def test_defaults_when_nothing_set(monkeypatch):
    for var in ("BEACON_URL", "BEACON_ADMIN_USERNAME", "BEACON_ADMIN_PASSWORD"):
        monkeypatch.delenv(var, raising=False)
    cfg = ClientConfig.resolve()
    assert cfg.url == DEFAULT_URL
    assert cfg.auth is None  # anonymous / read-only by default


def test_trailing_slash_stripped():
    assert ClientConfig(url="http://h:5001/").url == "http://h:5001"


def test_explicit_overrides_env(monkeypatch):
    monkeypatch.setenv("BEACON_URL", "http://from-env:1")
    cfg = ClientConfig.resolve(url="http://explicit:2")
    assert cfg.url == "http://explicit:2"


def test_env_fallback(monkeypatch):
    monkeypatch.setenv("BEACON_URL", "http://from-env:1")
    monkeypatch.setenv("BEACON_ADMIN_USERNAME", "admin")
    monkeypatch.setenv("BEACON_ADMIN_PASSWORD", "secret")
    cfg = ClientConfig.resolve()
    assert cfg.url == "http://from-env:1"
    assert cfg.auth == ("admin", "secret")


def test_auth_requires_both_parts():
    assert ClientConfig(username="admin").auth is None
    assert ClientConfig(password="secret").auth is None
    assert ClientConfig(username="admin", password="secret").auth == ("admin", "secret")

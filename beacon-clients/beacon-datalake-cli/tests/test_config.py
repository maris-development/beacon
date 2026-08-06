"""Tests for connection config resolution and auth handling."""

from __future__ import annotations

from beacon_datalake_cli.config import DEFAULT_URL, ClientConfig


def test_defaults_when_nothing_set():
    cfg = ClientConfig.resolve()
    assert cfg.url == DEFAULT_URL
    assert cfg.url == "http://127.0.0.1:5001"  # IPv4 to avoid the localhost stall
    assert cfg.auth is None  # anonymous / read-only by default


def test_trailing_slash_stripped():
    assert ClientConfig(url="http://h:5001/").url == "http://h:5001"


def test_localhost_is_rewritten_to_ipv4():
    # Avoids the ~2s Windows IPv6 (::1) connect stall when the server is IPv4-only.
    assert ClientConfig(url="http://localhost:5001").url == "http://127.0.0.1:5001"
    assert ClientConfig(url="https://localhost").url == "https://127.0.0.1"
    assert ClientConfig(url="http://localhost:5001/").url == "http://127.0.0.1:5001"


def test_non_localhost_hosts_are_untouched():
    assert ClientConfig(url="http://beacon.example.com:5001").url == "http://beacon.example.com:5001"
    assert ClientConfig(url="http://localhost.internal:5001").url == "http://localhost.internal:5001"


def test_explicit_arguments_used():
    cfg = ClientConfig.resolve(url="http://explicit:2", username="admin", password="secret")
    assert cfg.url == "http://explicit:2"
    assert cfg.auth == ("admin", "secret")


def test_env_vars_are_ignored(monkeypatch):
    # Connection details come only from explicit arguments; the server's BEACON_*
    # env vars must never leak into the CLI session.
    monkeypatch.setenv("BEACON_URL", "http://from-env:1")
    monkeypatch.setenv("BEACON_ADMIN_USERNAME", "admin")
    monkeypatch.setenv("BEACON_ADMIN_PASSWORD", "secret")
    cfg = ClientConfig.resolve()
    assert cfg.url == DEFAULT_URL
    assert cfg.auth is None


def test_auth_requires_both_parts():
    assert ClientConfig(username="admin").auth is None
    assert ClientConfig(password="secret").auth is None
    assert ClientConfig(username="admin", password="secret").auth == ("admin", "secret")

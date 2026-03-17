"""Tests for elt_common.catalog"""

import os

import pytest

from elt_common.catalog import CatalogConfig, CatalogCredentials


class TestCatalogCredentials:
    def test_validates_partial_auth_fields_raises(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__URI", "http://localhost")
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__CLIENT_ID", "my-id")
        # missing client_secret and oauth2_server_uri
        with pytest.raises(Exception, match="Missing required authentication"):
            CatalogCredentials()

    def test_all_auth_fields_accepted(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__URI", "http://localhost")
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__CLIENT_ID", "my-id")
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__CLIENT_SECRET", "s3cret")
        monkeypatch.setenv(
            "DESTINATION__PYICEBERG__CREDENTIALS__OAUTH2_SERVER_URI", "http://auth/token"
        )
        creds = CatalogCredentials()
        assert creds.uri == "http://localhost"
        assert creds.client_id.get_secret_value() == "my-id"

    def test_no_auth_fields_accepted(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__URI", "http://localhost")
        creds = CatalogCredentials()
        assert creds.client_id is None


class TestCatalogConfig:
    def test_defaults_to_rest(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__URI", "http://localhost")
        config = CatalogConfig()
        assert config.catalog_type == "rest"

    def test_sql_connection_properties(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__PYICEBERG__CATALOG_TYPE", "sql")
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__URI", "sqlite:///test.db")
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__WAREHOUSE", "file:///tmp/wh")
        config = CatalogConfig()
        props = config.connection_properties()

        assert props["type"] == "sql"
        assert props["uri"] == "sqlite:///test.db"
        assert props["warehouse"] == "file:///tmp/wh"

    def test_rest_connection_properties_with_auth(self, monkeypatch):
        monkeypatch.setenv("DESTINATION__PYICEBERG__CATALOG_TYPE", "rest")
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__URI", "http://catalog:8080")
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__WAREHOUSE", "my-wh")
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__CLIENT_ID", "cid")
        monkeypatch.setenv("DESTINATION__PYICEBERG__CREDENTIALS__CLIENT_SECRET", "csec")
        monkeypatch.setenv(
            "DESTINATION__PYICEBERG__CREDENTIALS__OAUTH2_SERVER_URI", "http://auth/token"
        )
        config = CatalogConfig()
        props = config.connection_properties()

        assert props["type"] == "rest"
        assert props["uri"] == "http://catalog:8080"
        assert props["warehouse"] == "my-wh"
        assert props["credential"] == "cid:csec"
        assert props["oauth2-server-uri"] == "http://auth/token"

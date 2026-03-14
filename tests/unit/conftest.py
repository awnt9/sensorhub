import os
from unittest.mock import MagicMock, patch

# Must be set before any app imports so Settings() doesn't fail
os.environ.update({
    "MINIO_ACCESS_KEY": "test",
    "MINIO_SECRET_KEY": "test",
    "MINIO_PORT": "9000",
    "MINIO_IP": "localhost",
    "MINIO_BUCKET": "test-bucket",
    "MONGO_USERNAME": "root",
    "MONGO_ROOT_PASSWORD": "test",
    "MONGO_PORT": "27017",
    "MONGO_IP": "localhost",
    "MONGO_DB": "sensorhub",
    "API_PORT": "8000",
})

import pytest
from fastapi.testclient import TestClient

# Patch MongoClient during initial app import so db = MongoDB() doesn't try to connect
with patch("sensorhub.mongo.MongoClient"):
    from sensorhub.api import app as _app


@pytest.fixture
def mock_db():
    with patch("sensorhub.api.db") as mock:
        yield mock


@pytest.fixture
def mock_minio():
    with patch("sensorhub.reports.MinioClient") as mock_cls:
        instance = MagicMock()
        mock_cls.return_value = instance
        yield instance


@pytest.fixture
def client(mock_db):
    return TestClient(_app)

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a SparkSession fixture for testing."""
    return SparkSession.getActiveSession()
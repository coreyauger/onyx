"""Test fixtures for Box connector tests."""

import pytest

from onyx.connectors.box.connector import BoxConnector


@pytest.fixture
def box_connector() -> BoxConnector:
    """Create a Box connector instance for testing."""
    return BoxConnector(
        include_all_files=True,
        folder_ids=None,
    )

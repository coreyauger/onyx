"""Basic tests for Box connector."""

import pytest

from onyx.connectors.box.connector import BoxConnector


@pytest.mark.skip(reason="Requires Box credentials and test environment setup")
def test_box_connector_initialization(box_connector: BoxConnector) -> None:
    """Test that Box connector can be initialized."""
    assert box_connector is not None
    assert box_connector.include_all_files is True


@pytest.mark.skip(reason="Requires Box credentials and test environment setup")
def test_box_connector_credentials(box_connector: BoxConnector) -> None:
    """Test that Box connector can load credentials."""
    # This test requires actual Box credentials
    # credentials = {
    #     "box_access_token": "test_token",
    #     "box_user_id": "test_user",
    # }
    # box_connector.load_credentials(credentials)
    # assert box_connector.box_client is not None


@pytest.mark.skip(reason="Requires Box credentials and test environment setup")
def test_box_connector_checkpoint(box_connector: BoxConnector) -> None:
    """Test that Box connector can create checkpoints."""
    checkpoint = box_connector.build_dummy_checkpoint()
    assert checkpoint is not None
    assert checkpoint.has_more is True

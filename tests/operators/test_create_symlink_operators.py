from unittest.mock import Mock, patch

from lakefs_sdk.models.storage_uri import StorageURI
from lakefs_sdk.client import LakeFSClient

from lakefs_provider.hooks.lakefs_hook import LakeFSHook
from lakefs_provider.operators.create_symlink_operator import (
    LakeFSCreateSymlinkOperator,
)


@patch.object(LakeFSHook, "get_conn")
def test_create_symlink_file(mock_conn):
    # Mock client
    mock_client = Mock(LakeFSClient)()
    mock_conn.return_value = mock_client

    # Mock response
    mock_client.internal_api.create_symlink_file.return_value = StorageURI.from_dict({
        "location": "file://path/to/symlink",
    })

    # Init Hook
    operator = LakeFSCreateSymlinkOperator(
        task_id="operator", lakefs_conn_id="", repo="repo", branch="branch"
    )

    # Call with required arguments
    assert operator.execute({}) == "file://path/to/symlink"
    mock_client.internal_api.create_symlink_file.assert_called_once_with(
        repository="repo", branch="branch"
    )


@patch.object(LakeFSHook, "get_conn")
def test_create_symlink_file_with_location(mock_conn):
    # Mock client
    mock_client = Mock(LakeFSClient)()
    mock_conn.return_value = mock_client
    mock_client.internal_api.create_symlink_file.return_value = {
        "location": "file://custom/path/to/symlink"
    }

    # Init Hook
    operator = LakeFSCreateSymlinkOperator(
        task_id="operator",
        lakefs_conn_id="",
        repo="repo",
        branch="branch",
        location="file://custom/path/to/symlink",
    )

    # Call with optional arguments
    assert operator.execute({}) == "file://custom/path/to/symlink"
    mock_client.internal_api.create_symlink_file.assert_called_once_with(
        repository="repo", branch="branch", location="file://custom/path/to/symlink"
    )

"""Regression tests for get_current_best bare tag matching.

Bug context: get_current_best() only matched tags with prefix pattern (e.g., "best-v1")
but not bare tags (e.g., "best"). Users expected @best to be found by get_experiment_context.
"""

import json
from unittest.mock import MagicMock


def test_get_current_best_matches_bare_best_tag():
    """Regression test: get_current_best must find bare 'best' tag (no hyphen suffix).

    Bug: get_current_best() used LIKE "best-%" which didn't match "best".
    Fix: Now matches both "best" and "best-*" patterns.
    """
    from goldfish.experiment_model.records import ExperimentRecordManager

    # Create mock database with the tag
    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

    # Setup: run_tags table has a "best" tag (no hyphen suffix)
    tag_row = {"tag_name": "best", "record_id": "01ABC123"}
    results_row = {"results_final": json.dumps({"primary_metric": "accuracy", "value": 0.95})}

    # First query returns the tag, second query returns results
    mock_conn.execute.return_value.fetchone.side_effect = [tag_row, results_row]

    manager = ExperimentRecordManager(mock_db)
    result = manager.get_current_best("test_workspace")

    # Should find the bare "best" tag
    assert result is not None, "get_current_best should find bare 'best' tag"
    assert result["tag"] == "best"
    assert result["record_id"] == "01ABC123"
    assert result["metric"] == "accuracy"
    assert result["value"] == 0.95


def test_get_current_best_still_matches_prefixed_tags():
    """Verify get_current_best still finds prefixed tags like 'best-v1'."""
    from goldfish.experiment_model.records import ExperimentRecordManager

    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

    # Setup: run_tags table has a "best-v1" tag (with hyphen suffix)
    tag_row = {"tag_name": "best-v1", "record_id": "01DEF456"}
    results_row = {"results_final": json.dumps({"primary_metric": "loss", "value": 0.05})}

    mock_conn.execute.return_value.fetchone.side_effect = [tag_row, results_row]

    manager = ExperimentRecordManager(mock_db)
    result = manager.get_current_best("test_workspace")

    # Should still find the prefixed tag
    assert result is not None, "get_current_best should still find 'best-v1' tag"
    assert result["tag"] == "best-v1"
    assert result["record_id"] == "01DEF456"


def test_get_current_best_falls_back_to_version_tags_for_bare_best():
    """Regression test: bare 'best' tag should also be found in workspace_version_tags."""
    from goldfish.experiment_model.records import ExperimentRecordManager

    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

    # First query (run_tags) returns None, second (version_tags) finds the tag
    version_tag_row = {"tag_name": "best", "record_id": "01GHI789"}
    results_row = {"results_final": json.dumps({"primary_metric": "f1_score", "value": 0.88})}

    mock_conn.execute.return_value.fetchone.side_effect = [
        None,  # run_tags query returns nothing
        version_tag_row,  # version_tags query finds the tag
        results_row,  # results query
    ]

    manager = ExperimentRecordManager(mock_db)
    result = manager.get_current_best("test_workspace")

    # Should find the bare "best" tag via version_tags fallback
    assert result is not None, "get_current_best should find bare 'best' in version_tags"
    assert result["tag"] == "best"
    assert result["record_id"] == "01GHI789"


def test_get_current_best_returns_none_when_no_best_tags():
    """Verify get_current_best returns None when no matching tags exist."""
    from goldfish.experiment_model.records import ExperimentRecordManager

    mock_db = MagicMock()
    mock_conn = MagicMock()
    mock_db._conn.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_db._conn.return_value.__exit__ = MagicMock(return_value=False)

    # Both queries return None
    mock_conn.execute.return_value.fetchone.side_effect = [None, None]

    manager = ExperimentRecordManager(mock_db)
    result = manager.get_current_best("test_workspace")

    assert result is None, "get_current_best should return None when no tags match"

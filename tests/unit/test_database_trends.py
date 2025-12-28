"""Unit tests for database metrics trends logic."""

import pytest

from goldfish.db.database import Database


@pytest.fixture
def test_db(tmp_path):
    db_path = tmp_path / "test_trends.db"
    db = Database(db_path)
    # Ensure schema is created
    with db._conn() as conn:
        # Manually verify schema or trust _init_db
        pass
    return db


def _create_dummy_run(conn, run_id):
    # Setup dependencies for FK constraints
    conn.execute(
        "INSERT OR IGNORE INTO workspace_lineage (workspace_name, created_at) VALUES ('w1', '2024-01-01T00:00:00Z')"
    )
    conn.execute(
        "INSERT OR IGNORE INTO workspace_versions (workspace_name, version, git_tag, git_sha, created_at, created_by) VALUES ('w1', 'v1', 'tag', 'sha', '2024-01-01T00:00:00Z', 'manual')"
    )
    conn.execute(
        "INSERT OR IGNORE INTO stage_versions (id, workspace_name, stage_name, version_num, git_sha, config_hash, created_at) VALUES (1, 'w1', 'test-stage', 1, 'sha', 'hash', '2024-01-01T00:00:00Z')"
    )

    conn.execute(
        """
        INSERT INTO stage_runs (
            id, workspace_name, stage_name, status, started_at, version, stage_version_id
        ) VALUES (?, 'w1', 'test-stage', 'running', '2024-01-01T09:00:00Z', 'v1', 1)
        """,
        (run_id,),
    )


def test_get_metrics_trends_logic(test_db):
    """Test that get_metrics_trends correctly identifies previous and last values."""
    run_id = "run-1"

    with test_db._conn() as conn:
        _create_dummy_run(conn, run_id)

    # Insert metrics
    # Metric A: 3 values (should get last 2)
    # Metric B: 1 value (should get 1)
    # Metric C: 2 values (should get 2)

    metrics = [
        ("A", 1.0, "2024-01-01T10:00:00Z"),
        ("A", 2.0, "2024-01-01T10:01:00Z"),
        ("A", 3.0, "2024-01-01T10:02:00Z"),  # Last
        ("B", 10.0, "2024-01-01T10:00:00Z"),
        ("C", 100.0, "2024-01-01T10:00:00Z"),
        ("C", 90.0, "2024-01-01T10:01:00Z"),
    ]

    with test_db._conn() as conn:
        for name, val, ts in metrics:
            conn.execute(
                "INSERT INTO run_metrics (stage_run_id, name, value, step, timestamp) VALUES (?, ?, ?, ?, ?)",
                (run_id, name, val, None, ts),
            )

    trends = test_db.get_metrics_trends(run_id)

    # Check Metric A (should be [2.0, 3.0] or [3.0, 2.0] depending on implementation sort order)
    # The SQL query uses: ORDER BY name, rank DESC
    # rank 1 is last, rank 2 is prev.
    # We want [prev, last] for trend calculation.
    # The implementation:
    # rows = conn.execute(...).fetchall()
    # for row in rows: trends[name].append(row["value"])
    # The SQL returns rank <= 2, ORDER BY name, rank DESC
    # rank 2 comes first (prev), then rank 1 (last).

    assert trends["A"] == [2.0, 3.0]
    assert trends["B"] == [10.0]
    assert trends["C"] == [100.0, 90.0]


def test_get_metrics_trends_filtering(test_db):
    """Test filtering by metric names."""
    run_id = "run-filter"
    with test_db._conn() as conn:
        _create_dummy_run(conn, run_id)
        conn.execute(
            "INSERT INTO run_metrics (stage_run_id, name, value, timestamp) VALUES (?, ?, ?, ?)",
            (run_id, "keep", 1.0, "2024-01-01T10:00:00Z"),
        )
        conn.execute(
            "INSERT INTO run_metrics (stage_run_id, name, value, timestamp) VALUES (?, ?, ?, ?)",
            (run_id, "ignore", 2.0, "2024-01-01T10:00:00Z"),
        )

    trends = test_db.get_metrics_trends(run_id, metric_names=["keep"])
    assert "keep" in trends
    assert "ignore" not in trends

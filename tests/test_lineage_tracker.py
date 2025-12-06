"""Comprehensive tests for LineageTracker module - P2.

Tests cover:
- Ancestor tracking
- Descendant tracking (reverse lineage)
- Full lineage graph generation
- Producing job identification
- Cycle detection and depth limiting
"""

import pytest


class TestLineageTrackerGetAncestors:
    """Tests for get_ancestors() method."""

    def test_get_ancestors_linear_chain(self, temp_dir):
        """Test tracing ancestors in a linear chain."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create linear chain: A -> B -> C
        db.create_source(
            source_id="source-a",
            name="source-a",
            gcs_location="gs://bucket/a",
            created_by="external",
        )
        db.create_source(
            source_id="source-b",
            name="source-b",
            gcs_location="gs://bucket/b",
            created_by="job:job-1",
        )
        db.add_lineage(source_id="source-b", parent_source_id="source-a", job_id="job-1")

        db.create_source(
            source_id="source-c",
            name="source-c",
            gcs_location="gs://bucket/c",
            created_by="job:job-2",
        )
        db.add_lineage(source_id="source-c", parent_source_id="source-b", job_id="job-2")

        # Get ancestors of C
        ancestors = tracker.get_ancestors("source-c")

        assert len(ancestors) == 2
        # Oldest first
        assert ancestors[0] == "source-a"
        assert ancestors[1] == "source-b"

    def test_get_ancestors_no_parents(self, temp_dir):
        """Test get_ancestors for a source with no parents."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create source with no parents
        db.create_source(
            source_id="source-root",
            name="source-root",
            gcs_location="gs://bucket/root",
            created_by="external",
        )

        ancestors = tracker.get_ancestors("source-root")

        assert len(ancestors) == 0

    def test_get_ancestors_multiple_parents(self, temp_dir):
        """Test get_ancestors with multiple parent sources."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create diamond structure:
        #     A
        #    / \
        #   B   C
        #    \ /
        #     D
        db.create_source("source-a", "source-a", "gs://bucket/a", "external")
        db.create_source("source-b", "source-b", "gs://bucket/b", "job:job-1")
        db.add_lineage("source-b", parent_source_id="source-a", job_id="job-1")

        db.create_source("source-c", "source-c", "gs://bucket/c", "job:job-2")
        db.add_lineage("source-c", parent_source_id="source-a", job_id="job-2")

        db.create_source("source-d", "source-d", "gs://bucket/d", "job:job-3")
        db.add_lineage("source-d", parent_source_id="source-b", job_id="job-3")
        db.add_lineage("source-d", parent_source_id="source-c", job_id="job-3")

        # Get ancestors of D
        ancestors = tracker.get_ancestors("source-d")

        # Should include A, B, C
        assert len(ancestors) == 3
        assert "source-a" in ancestors
        assert "source-b" in ancestors
        assert "source-c" in ancestors

    def test_get_ancestors_respects_max_depth(self, temp_dir):
        """Test that max_depth parameter limits recursion."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create deep chain: A -> B -> C -> D -> E
        db.create_source("source-a", "source-a", "gs://bucket/a", "external")

        db.create_source("source-b", "source-b", "gs://bucket/b", "job:job-1")
        db.add_lineage("source-b", parent_source_id="source-a", job_id="job-1")

        db.create_source("source-c", "source-c", "gs://bucket/c", "job:job-2")
        db.add_lineage("source-c", parent_source_id="source-b", job_id="job-2")

        db.create_source("source-d", "source-d", "gs://bucket/d", "job:job-3")
        db.add_lineage("source-d", parent_source_id="source-c", job_id="job-3")

        db.create_source("source-e", "source-e", "gs://bucket/e", "job:job-4")
        db.add_lineage("source-e", parent_source_id="source-d", job_id="job-4")

        # Get ancestors with max_depth=2
        ancestors = tracker.get_ancestors("source-e", max_depth=2)

        # Should only go 2 levels up: D and C
        assert len(ancestors) <= 2

    def test_get_ancestors_nonexistent_source(self, temp_dir):
        """Test that get_ancestors raises error for non-existent source."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker
        from goldfish.errors import SourceNotFoundError

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        with pytest.raises(SourceNotFoundError):
            tracker.get_ancestors("nonexistent-source")


class TestLineageTrackerGetDescendants:
    """Tests for get_descendants() method - reverse lineage."""

    def test_get_descendants_linear_chain(self, temp_dir):
        """Test finding descendants in a linear chain."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create linear chain: A -> B -> C
        db.create_source("source-a", "source-a", "gs://bucket/a", "external")
        db.create_source("source-b", "source-b", "gs://bucket/b", "job:job-1")
        db.add_lineage("source-b", parent_source_id="source-a", job_id="job-1")

        db.create_source("source-c", "source-c", "gs://bucket/c", "job:job-2")
        db.add_lineage("source-c", parent_source_id="source-b", job_id="job-2")

        # Get descendants of A
        descendants = tracker.get_descendants("source-a")

        assert len(descendants) == 2
        # Should include both B and C
        assert "source-b" in descendants
        assert "source-c" in descendants

    def test_get_descendants_no_children(self, temp_dir):
        """Test get_descendants for a leaf source with no children."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create leaf source
        db.create_source("source-leaf", "source-leaf", "gs://bucket/leaf", "external")

        descendants = tracker.get_descendants("source-leaf")

        assert len(descendants) == 0

    def test_get_descendants_multiple_children(self, temp_dir):
        """Test get_descendants with branching lineage."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create tree:
        #     A
        #    / \
        #   B   C
        #  /
        # D
        db.create_source("source-a", "source-a", "gs://bucket/a", "external")
        db.create_source("source-b", "source-b", "gs://bucket/b", "job:job-1")
        db.add_lineage("source-b", parent_source_id="source-a", job_id="job-1")

        db.create_source("source-c", "source-c", "gs://bucket/c", "job:job-2")
        db.add_lineage("source-c", parent_source_id="source-a", job_id="job-2")

        db.create_source("source-d", "source-d", "gs://bucket/d", "job:job-3")
        db.add_lineage("source-d", parent_source_id="source-b", job_id="job-3")

        # Get descendants of A
        descendants = tracker.get_descendants("source-a")

        # Should include B, C, D
        assert len(descendants) == 3
        assert "source-b" in descendants
        assert "source-c" in descendants
        assert "source-d" in descendants

    def test_get_descendants_respects_max_depth(self, temp_dir):
        """Test that max_depth limits descendant search."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create deep chain: A -> B -> C -> D
        db.create_source("source-a", "source-a", "gs://bucket/a", "external")
        db.create_source("source-b", "source-b", "gs://bucket/b", "job:job-1")
        db.add_lineage("source-b", parent_source_id="source-a", job_id="job-1")

        db.create_source("source-c", "source-c", "gs://bucket/c", "job:job-2")
        db.add_lineage("source-c", parent_source_id="source-b", job_id="job-2")

        db.create_source("source-d", "source-d", "gs://bucket/d", "job:job-3")
        db.add_lineage("source-d", parent_source_id="source-c", job_id="job-3")

        # Get descendants with max_depth=2
        descendants = tracker.get_descendants("source-a", max_depth=2)

        # Should only go 2 levels down: B and C
        assert len(descendants) <= 2

    def test_get_descendants_nonexistent_source(self, temp_dir):
        """Test that get_descendants raises error for non-existent source."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker
        from goldfish.errors import SourceNotFoundError

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        with pytest.raises(SourceNotFoundError):
            tracker.get_descendants("nonexistent-source")


class TestLineageTrackerFullGraph:
    """Tests for get_full_lineage_graph() method."""

    def test_get_full_lineage_graph_structure(self, temp_dir):
        """Test that full graph includes correct structure."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create simple chain: A -> B -> C
        db.create_source("source-a", "source-a", "gs://bucket/a", "external")
        db.create_source("source-b", "source-b", "gs://bucket/b", "job:job-1")
        db.add_lineage("source-b", parent_source_id="source-a", job_id="job-1")

        db.create_source("source-c", "source-c", "gs://bucket/c", "job:job-2")
        db.add_lineage("source-c", parent_source_id="source-b", job_id="job-2")

        # Get full graph centered on B
        graph = tracker.get_full_lineage_graph("source-b")

        # Should have correct structure
        assert "center" in graph
        assert graph["center"] == "source-b"

        assert "nodes" in graph
        assert "edges" in graph

        # Nodes should include A (ancestor), B (center), C (descendant)
        assert len(graph["nodes"]) == 3
        assert "source-a" in graph["nodes"]
        assert "source-b" in graph["nodes"]
        assert "source-c" in graph["nodes"]

        # Edges should represent relationships
        assert len(graph["edges"]) == 2

    def test_get_full_lineage_graph_edges_format(self, temp_dir):
        """Test that edges have correct format with job IDs."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create A -> B
        db.create_source("source-a", "source-a", "gs://bucket/a", "external")
        db.create_source("source-b", "source-b", "gs://bucket/b", "job:job-1")
        db.add_lineage("source-b", parent_source_id="source-a", job_id="job-1")

        graph = tracker.get_full_lineage_graph("source-b")

        # Check edge format
        assert len(graph["edges"]) == 1
        edge = graph["edges"][0]

        assert "from" in edge
        assert "to" in edge
        assert "job_id" in edge

        assert edge["from"] == "source-a"
        assert edge["to"] == "source-b"
        assert edge["job_id"] == "job-1"

    def test_get_full_lineage_graph_isolated_node(self, temp_dir):
        """Test graph for isolated node with no lineage."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create isolated source
        db.create_source("source-alone", "source-alone", "gs://bucket/alone", "external")

        graph = tracker.get_full_lineage_graph("source-alone")

        # Should have just the center node
        assert len(graph["nodes"]) == 1
        assert graph["nodes"][0] == "source-alone"
        assert len(graph["edges"]) == 0

    def test_get_full_lineage_graph_nonexistent_source(self, temp_dir):
        """Test that full graph raises error for non-existent source."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker
        from goldfish.errors import SourceNotFoundError

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        with pytest.raises(SourceNotFoundError):
            tracker.get_full_lineage_graph("nonexistent-source")


class TestLineageTrackerProducingJob:
    """Tests for get_producing_job() method."""

    def test_get_producing_job_from_job(self, temp_dir):
        """Test extracting job ID from job-produced source."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create source produced by a job
        db.create_source(
            source_id="source-from-job",
            name="source-from-job",
            gcs_location="gs://bucket/from-job",
            created_by="job:job-abc123",
        )

        job_id = tracker.get_producing_job("source-from-job")

        assert job_id == "job-abc123"

    def test_get_producing_job_external_source(self, temp_dir):
        """Test that external sources return None."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create external source
        db.create_source(
            source_id="source-external",
            name="source-external",
            gcs_location="gs://bucket/external",
            created_by="external",
        )

        job_id = tracker.get_producing_job("source-external")

        assert job_id is None

    def test_get_producing_job_nonexistent_source(self, temp_dir):
        """Test that get_producing_job raises error for non-existent source."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker
        from goldfish.errors import SourceNotFoundError

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        with pytest.raises(SourceNotFoundError):
            tracker.get_producing_job("nonexistent-source")


class TestLineageTrackerCycleDetection:
    """Tests for cycle detection and depth limiting."""

    def test_max_depth_prevents_infinite_loops(self, temp_dir):
        """Test that max_depth prevents infinite recursion."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create very deep chain
        prev_source = None
        for i in range(20):
            source_id = f"source-{i}"
            db.create_source(
                source_id=source_id,
                name=source_id,
                gcs_location=f"gs://bucket/{source_id}",
                created_by="external" if i == 0 else f"job:job-{i}",
            )
            if prev_source:
                db.add_lineage(source_id, parent_source_id=prev_source, job_id=f"job-{i}")
            prev_source = source_id

        # Get ancestors with default max_depth=10
        ancestors = tracker.get_ancestors("source-19", max_depth=10)

        # Should stop at depth 10
        assert len(ancestors) <= 10

    def test_visited_set_prevents_duplicate_processing(self, temp_dir):
        """Test that visited set prevents processing same node twice."""
        from goldfish.db.database import Database
        from goldfish.sources.lineage import LineageTracker

        db = Database(temp_dir / "test.db")
        tracker = LineageTracker(db)

        # Create diamond with shared ancestor:
        #     A
        #    / \
        #   B   C
        #    \ /
        #     D
        db.create_source("source-a", "source-a", "gs://bucket/a", "external")
        db.create_source("source-b", "source-b", "gs://bucket/b", "job:job-1")
        db.add_lineage("source-b", parent_source_id="source-a", job_id="job-1")

        db.create_source("source-c", "source-c", "gs://bucket/c", "job:job-2")
        db.add_lineage("source-c", parent_source_id="source-a", job_id="job-2")

        db.create_source("source-d", "source-d", "gs://bucket/d", "job:job-3")
        db.add_lineage("source-d", parent_source_id="source-b", job_id="job-3")
        db.add_lineage("source-d", parent_source_id="source-c", job_id="job-3")

        # Get ancestors of D
        ancestors = tracker.get_ancestors("source-d")

        # A should appear only once even though it's reached via two paths
        assert ancestors.count("source-a") == 1

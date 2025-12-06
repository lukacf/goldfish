"""Lineage tracking utilities.

Provides tools for tracing data lineage through the pipeline.
"""

from typing import Optional

from goldfish.db.database import Database
from goldfish.errors import SourceNotFoundError


class LineageTracker:
    """Tracks and queries data lineage."""

    def __init__(self, db: Database):
        """Initialize lineage tracker.

        Args:
            db: Database instance
        """
        self.db = db

    def get_ancestors(self, source_name: str, max_depth: int = 10) -> list[str]:
        """Get all ancestor sources (recursive).

        Traces back through the lineage to find all sources
        that contributed to this one.

        Args:
            source_name: Source to trace from
            max_depth: Maximum recursion depth

        Returns:
            List of ancestor source names (oldest first)
        """
        if not self.db.source_exists(source_name):
            raise SourceNotFoundError(f"Source not found: {source_name}")

        ancestors = []
        visited = set()
        self._collect_ancestors(source_name, ancestors, visited, max_depth)
        return ancestors

    def _collect_ancestors(
        self,
        source_name: str,
        ancestors: list[str],
        visited: set[str],
        remaining_depth: int,
    ) -> None:
        """Recursively collect ancestors."""
        if remaining_depth <= 0 or source_name in visited:
            return

        visited.add(source_name)
        lineage = self.db.get_lineage(source_name)

        for record in lineage:
            parent = record.get("parent_source_id")
            if parent and parent not in visited:
                # Recurse first to get oldest ancestors first
                self._collect_ancestors(parent, ancestors, visited, remaining_depth - 1)
                ancestors.append(parent)

    def get_descendants(self, source_name: str, max_depth: int = 10) -> list[str]:
        """Get all descendant sources (recursive).

        Finds all sources that were derived from this one.

        Args:
            source_name: Source to trace from
            max_depth: Maximum recursion depth

        Returns:
            List of descendant source names (newest last)
        """
        if not self.db.source_exists(source_name):
            raise SourceNotFoundError(f"Source not found: {source_name}")

        descendants = []
        visited = set()
        self._collect_descendants(source_name, descendants, visited, max_depth)
        return descendants

    def _collect_descendants(
        self,
        source_name: str,
        descendants: list[str],
        visited: set[str],
        remaining_depth: int,
    ) -> None:
        """Recursively collect descendants."""
        if remaining_depth <= 0 or source_name in visited:
            return

        visited.add(source_name)

        # Find all sources that have this source as a parent
        # This requires a reverse lookup - for now we scan all sources
        all_sources = self.db.list_sources()

        for source in all_sources:
            child_name = source["name"]
            if child_name in visited:
                continue

            lineage = self.db.get_lineage(child_name)
            for record in lineage:
                if record.get("parent_source_id") == source_name:
                    descendants.append(child_name)
                    self._collect_descendants(
                        child_name, descendants, visited, remaining_depth - 1
                    )
                    break

    def get_full_lineage_graph(self, source_name: str) -> dict:
        """Get full lineage graph for visualization.

        Returns a graph structure showing the complete lineage
        both up (ancestors) and down (descendants).

        Args:
            source_name: Source to center the graph on

        Returns:
            Dict with nodes and edges for visualization
        """
        if not self.db.source_exists(source_name):
            raise SourceNotFoundError(f"Source not found: {source_name}")

        nodes = set()
        edges = []

        # Get ancestors
        ancestors = self.get_ancestors(source_name)
        nodes.update(ancestors)
        nodes.add(source_name)

        # Get descendants
        descendants = self.get_descendants(source_name)
        nodes.update(descendants)

        # Build edges from lineage records
        for node in nodes:
            lineage = self.db.get_lineage(node)
            for record in lineage:
                parent = record.get("parent_source_id")
                if parent and parent in nodes:
                    edges.append({
                        "from": parent,
                        "to": node,
                        "job_id": record.get("job_id"),
                    })

        return {
            "center": source_name,
            "nodes": list(nodes),
            "edges": edges,
        }

    def get_producing_job(self, source_name: str) -> Optional[str]:
        """Get the job that produced a source.

        Args:
            source_name: Source name

        Returns:
            Job ID if source was produced by a job, None otherwise
        """
        if not self.db.source_exists(source_name):
            raise SourceNotFoundError(f"Source not found: {source_name}")

        source = self.db.get_source(source_name)
        created_by = source.get("created_by", "")

        if created_by.startswith("job:"):
            return created_by[4:]  # Strip "job:" prefix

        return None

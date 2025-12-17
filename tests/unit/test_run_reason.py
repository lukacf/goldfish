"""Unit tests for RunReason model."""

import pytest
from pydantic import ValidationError

from goldfish.models import RunReason


class TestRunReasonValidation:
    """Tests for RunReason field validation."""

    def test_with_description_only(self) -> None:
        """Minimal valid RunReason with just description."""
        reason = RunReason(description="Testing new architecture")
        assert reason.description == "Testing new architecture"
        assert reason.hypothesis is None
        assert reason.approach is None
        assert reason.min_result is None
        assert reason.goal is None

    def test_with_all_fields(self) -> None:
        """Complete RunReason with all fields populated."""
        reason = RunReason(
            description="Testing dropout change",
            hypothesis="Lower dropout will improve accuracy",
            approach="Changed dropout from 0.5 to 0.3",
            min_result="No worse than baseline",
            goal="2-3% accuracy improvement",
        )
        assert reason.description == "Testing dropout change"
        assert reason.hypothesis == "Lower dropout will improve accuracy"
        assert reason.approach == "Changed dropout from 0.5 to 0.3"
        assert reason.min_result == "No worse than baseline"
        assert reason.goal == "2-3% accuracy improvement"

    def test_requires_description(self) -> None:
        """Description is required - missing raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            RunReason()  # type: ignore[call-arg]
        assert "description" in str(exc_info.value)

    def test_description_max_length(self) -> None:
        """Description has max_length=500."""
        # At limit - should work
        reason = RunReason(description="x" * 500)
        assert len(reason.description) == 500

        # Over limit - should fail
        with pytest.raises(ValidationError) as exc_info:
            RunReason(description="x" * 501)
        assert "description" in str(exc_info.value)

    def test_hypothesis_max_length(self) -> None:
        """Hypothesis has max_length=1000."""
        # At limit - should work
        reason = RunReason(description="test", hypothesis="h" * 1000)
        assert len(reason.hypothesis) == 1000  # type: ignore[arg-type]

        # Over limit - should fail
        with pytest.raises(ValidationError) as exc_info:
            RunReason(description="test", hypothesis="h" * 1001)
        assert "hypothesis" in str(exc_info.value)

    def test_approach_max_length(self) -> None:
        """Approach has max_length=1000."""
        reason = RunReason(description="test", approach="a" * 1000)
        assert len(reason.approach) == 1000  # type: ignore[arg-type]

        with pytest.raises(ValidationError):
            RunReason(description="test", approach="a" * 1001)

    def test_min_result_max_length(self) -> None:
        """min_result has max_length=500."""
        reason = RunReason(description="test", min_result="m" * 500)
        assert len(reason.min_result) == 500  # type: ignore[arg-type]

        with pytest.raises(ValidationError):
            RunReason(description="test", min_result="m" * 501)

    def test_goal_max_length(self) -> None:
        """goal has max_length=500."""
        reason = RunReason(description="test", goal="g" * 500)
        assert len(reason.goal) == 500  # type: ignore[arg-type]

        with pytest.raises(ValidationError):
            RunReason(description="test", goal="g" * 501)


class TestRunReasonToSummary:
    """Tests for to_summary() method."""

    def test_description_only(self) -> None:
        """Summary with just description."""
        reason = RunReason(description="Testing architecture")
        assert reason.to_summary() == "Testing architecture"

    def test_with_hypothesis(self) -> None:
        """Summary includes hypothesis with H: prefix."""
        reason = RunReason(
            description="Testing dropout",
            hypothesis="Will improve accuracy",
        )
        assert reason.to_summary() == "Testing dropout | H: Will improve accuracy"

    def test_ignores_other_fields(self) -> None:
        """Only description and hypothesis appear in summary."""
        reason = RunReason(
            description="Test",
            hypothesis="Expect improvement",
            approach="Changed config",
            min_result="No regression",
            goal="+5%",
        )
        summary = reason.to_summary()
        assert "Test" in summary
        assert "Expect improvement" in summary
        assert "Changed config" not in summary
        assert "No regression" not in summary
        assert "+5%" not in summary


class TestRunReasonToMarkdown:
    """Tests for to_markdown() method."""

    def test_description_only(self) -> None:
        """Markdown with just description."""
        reason = RunReason(description="Testing architecture")
        md = reason.to_markdown()
        assert "**Description:** Testing architecture" in md
        assert "**Hypothesis:**" not in md

    def test_all_fields(self) -> None:
        """Markdown includes all provided fields."""
        reason = RunReason(
            description="Test",
            hypothesis="Expect X",
            approach="Do Y",
            min_result="At least Z",
            goal="Ideally W",
        )
        md = reason.to_markdown()
        assert "**Description:** Test" in md
        assert "**Hypothesis:** Expect X" in md
        assert "**Approach:** Do Y" in md
        assert "**Min Result:** At least Z" in md
        assert "**Goal:** Ideally W" in md

    def test_field_order(self) -> None:
        """Fields appear in logical order."""
        reason = RunReason(
            description="Test",
            hypothesis="H",
            approach="A",
            min_result="M",
            goal="G",
        )
        md = reason.to_markdown()
        lines = md.split("\n")
        assert "Description" in lines[0]
        assert "Hypothesis" in lines[1]
        assert "Approach" in lines[2]
        assert "Min Result" in lines[3]
        assert "Goal" in lines[4]


class TestRunReasonEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_description_fails(self) -> None:
        """Empty string description should still create (min length validated elsewhere)."""
        # Pydantic allows empty string, min length is validated at tool level
        reason = RunReason(description="")
        assert reason.description == ""

    def test_whitespace_description(self) -> None:
        """Whitespace-only description is accepted by model."""
        reason = RunReason(description="   ")
        assert reason.description == "   "

    def test_extra_fields_ignored(self) -> None:
        """Unknown fields are silently ignored by Pydantic (default behavior)."""
        # By default Pydantic ignores extra fields. This is acceptable
        # as it provides flexibility for forward compatibility.
        reason = RunReason(description="test", unknown_field="value")  # type: ignore[call-arg]
        assert reason.description == "test"
        # Extra field is not stored
        assert not hasattr(reason, "unknown_field")

    def test_unicode_in_fields(self) -> None:
        """Unicode characters work in all fields."""
        reason = RunReason(
            description="Testing 🚀 architecture",
            hypothesis="希望提高准确性",
            approach="Изменить конфиг",
        )
        assert "🚀" in reason.description
        assert "希望" in reason.hypothesis  # type: ignore[operator]
        assert "Изменить" in reason.approach  # type: ignore[operator]

    def test_special_characters(self) -> None:
        """Special characters (quotes, newlines) work."""
        reason = RunReason(
            description='Testing "quoted" text',
            hypothesis="Line 1\nLine 2",
        )
        assert '"quoted"' in reason.description
        assert "\n" in reason.hypothesis  # type: ignore[operator]

    def test_serialization_field_names(self) -> None:
        """Serialization uses the canonical short field names."""
        reason = RunReason(
            description="test",
            min_result="min",
            goal="opt",
        )
        data = reason.model_dump()
        assert "min_result" in data
        assert "goal" in data
        assert data["min_result"] == "min"
        assert data["goal"] == "opt"

"""Unit tests for GCP Metadata Bus command syntax."""

from unittest.mock import MagicMock, patch

from goldfish.infra.metadata.base import MetadataSignal
from goldfish.infra.metadata.gcp import GCPMetadataBus


def test_set_signal_command_syntax():
    """Verify that set_signal uses correct gcloud flags to avoid escaping issues."""
    bus = GCPMetadataBus()
    sig = MetadataSignal(command="sync", request_id="12345")

    with patch("subprocess.run") as mock_run, patch("tempfile.NamedTemporaryFile") as mock_temp:
        # Setup mock temp file
        mock_file = MagicMock()
        mock_file.name = "/tmp/fake-metadata-file"
        mock_temp.return_value.__enter__.return_value = mock_file

        mock_run.return_value = MagicMock(stdout="", stderr="", returncode=0)

        bus.set_signal("goldfish", sig, target="test-instance")

        # Verify gcloud call
        assert mock_run.called
        args = mock_run.call_args[0][0]

        # It should NOT use --metadata KEY=VALUE because JSON breaks it
        # It SHOULD use --metadata-from-file KEY=FILE
        assert "add-metadata" in args
        assert "--metadata-from-file" in args
        assert any("goldfish=" in arg for arg in args)

        # Check that the file was written with the JSON signal
        mock_file.write.assert_called()
        written_content = mock_file.write.call_args[0][0]
        if isinstance(written_content, bytes):
            written_content = written_content.decode()
        assert '"request_id": "12345"' in written_content or '"request_id":"12345"' in written_content


def test_set_signal_with_zonal_target_adds_zone_flag() -> None:
    """Zonal instance targets should be parsed into instance + --zone flag."""
    bus = GCPMetadataBus()
    sig = MetadataSignal(command="sync", request_id="12345")

    with patch("subprocess.run") as mock_run, patch("tempfile.NamedTemporaryFile") as mock_temp:
        mock_file = MagicMock()
        mock_file.name = "/tmp/fake-metadata-file"
        mock_temp.return_value.__enter__.return_value = mock_file
        mock_run.return_value = MagicMock(stdout="", stderr="", returncode=0)

        bus.set_signal("goldfish", sig, target="zones/us-central1-a/instances/test-instance")

        args = mock_run.call_args[0][0]
        assert "test-instance" in args
        assert "--zone=us-central1-a" in args
        assert "zones/us-central1-a/instances/test-instance" not in args


def test_set_signal_with_project_zonal_target_adds_project_flag() -> None:
    """Project+zone instance targets should be parsed into flags."""
    bus = GCPMetadataBus()
    sig = MetadataSignal(command="sync", request_id="12345")

    with patch("subprocess.run") as mock_run, patch("tempfile.NamedTemporaryFile") as mock_temp:
        mock_file = MagicMock()
        mock_file.name = "/tmp/fake-metadata-file"
        mock_temp.return_value.__enter__.return_value = mock_file
        mock_run.return_value = MagicMock(stdout="", stderr="", returncode=0)

        bus.set_signal(
            "goldfish",
            sig,
            target="projects/my-proj/zones/us-central1-a/instances/test-instance",
        )

        args = mock_run.call_args[0][0]
        assert "test-instance" in args
        assert "--zone=us-central1-a" in args
        assert "--project=my-proj" in args

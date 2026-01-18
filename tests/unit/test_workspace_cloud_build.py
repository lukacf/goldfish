"""Unit tests for workspace Cloud Build functionality in DockerBuilder."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from goldfish.config import DockerConfig, GoldfishConfig
from goldfish.errors import CloudBuildError, CloudBuildNotConfiguredError
from goldfish.infra.docker_builder import DockerBuilder


@pytest.fixture
def mock_config() -> GoldfishConfig:
    """Create mock config without GCE."""
    config = MagicMock(spec=GoldfishConfig)
    config.gce = None
    config.docker = DockerConfig()
    config.jobs = MagicMock()
    config.jobs.backend = "local"
    config.svs = None
    return config


@pytest.fixture
def mock_config_with_gce() -> GoldfishConfig:
    """Create mock config with GCE configured."""
    config = MagicMock(spec=GoldfishConfig)
    gce = MagicMock()
    gce.project_id = "my-gcp-project"
    gce.effective_artifact_registry = "us-docker.pkg.dev/my-gcp-project/goldfish"
    config.gce = gce
    config.docker = DockerConfig()
    config.docker.cloud_build = MagicMock()
    config.docker.cloud_build.machine_type = "E2_HIGHCPU_32"
    config.docker.cloud_build.timeout_minutes = 30
    config.docker.cloud_build.disk_size_gb = 100
    config.jobs = MagicMock()
    config.jobs.backend = "gce"
    config.svs = None
    return config


@pytest.fixture
def mock_db():
    """Create a mock database."""
    db = MagicMock()
    db.insert_docker_build = MagicMock()
    db.update_docker_build_status = MagicMock()
    db.get_docker_build_by_workspace = MagicMock(return_value=None)
    # Must return None to not short-circuit build via content hash cache
    db.get_docker_build_by_content_hash = MagicMock(return_value=None)
    db.get_latest_docker_build_for_workspace = MagicMock(return_value=None)
    return db


@pytest.fixture
def workspace_dir(tmp_path: Path) -> Path:
    """Create a minimal workspace directory."""
    ws = tmp_path / "workspace"
    ws.mkdir()
    (ws / "modules").mkdir()
    (ws / "configs").mkdir()
    (ws / "modules" / "train.py").write_text("# train module")
    return ws


class TestBuildImageBackendSelection:
    """Tests for build_image backend parameter."""

    def test_build_image_defaults_to_local_backend(self, workspace_dir: Path, mock_config: GoldfishConfig):
        """build_image without backend parameter should use local."""
        builder = DockerBuilder(config=mock_config)

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            result = builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="test_ws",
                version="v1",
            )

        assert result.startswith("goldfish-")
        # Should have called docker build
        docker_calls = [c for c in mock_run.call_args_list if "docker" in str(c)]
        assert len(docker_calls) == 1

    def test_build_image_with_cloud_backend_requires_gce(
        self, workspace_dir: Path, mock_config: GoldfishConfig, mock_db
    ):
        """build_image with backend='cloud' should raise if no GCE config."""
        builder = DockerBuilder(config=mock_config, db=mock_db)

        with pytest.raises(CloudBuildNotConfiguredError):
            builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="test_ws",
                version="v1",
                backend="cloud",
            )

    def test_build_image_cloud_backend_returns_registry_tag(
        self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db
    ):
        """build_image with backend='cloud' should return registry tag on success."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        # Mock successful cloud build submission and completion
        def mock_subprocess(cmd, **kwargs):
            # Image existence check - return not found to trigger build
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(returncode=1, stdout="", stderr="NOT_FOUND")
            # Cloud Build submission
            if "builds" in cmd and "submit" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"id": "cloud-build-123", "status": "QUEUED"}),
                    stderr="",
                )
            # Cloud Build polling
            if "builds" in cmd and "describe" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"status": "SUCCESS", "logUrl": "https://logs.url"}),
                    stderr="",
                )
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=mock_subprocess):
            result = builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="test_ws",
                version="v1",
                backend="cloud",
                wait=True,
            )

        # Should return full registry tag
        assert "us-docker.pkg.dev/my-gcp-project/goldfish" in result
        assert "goldfish-test_ws-v1" in result


class TestCloudBuildSubmission:
    """Tests for _build_with_cloud_build method."""

    def test_cloud_build_creates_correct_cloudbuild_yaml(
        self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db
    ):
        """Cloud build should generate cloudbuild.yaml with correct structure."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        captured_config = {}

        def capture_submit(cmd, **kwargs):
            # Image existence check - return not found to trigger build
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(returncode=1, stdout="", stderr="NOT_FOUND")
            # Find the cloudbuild.yaml path from the command
            if "builds" in cmd and "submit" in cmd:
                for i, arg in enumerate(cmd):
                    if arg == "--config":
                        config_path = Path(cmd[i + 1])
                        import yaml

                        captured_config["content"] = yaml.safe_load(config_path.read_text())
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"id": "build-123"}),
                    stderr="",
                )
            elif "builds" in cmd and "describe" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"status": "SUCCESS", "logUrl": "url"}),
                    stderr="",
                )
            return MagicMock(returncode=0, stdout="", stderr="")

        with patch("subprocess.run", side_effect=capture_submit):
            builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="test_ws",
                version="v1",
                backend="cloud",
                wait=True,
            )

        assert "content" in captured_config
        config = captured_config["content"]

        # Check structure
        assert "steps" in config
        assert "images" in config
        assert "timeout" in config
        assert "options" in config

        # Check timeout (30 min = 1800s)
        assert config["timeout"] == "1800s"

        # Check machine type
        assert config["options"]["machineType"] == "E2_HIGHCPU_32"

    def test_cloud_build_submission_failure_raises_error(
        self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db
    ):
        """Cloud build submission failure should raise CloudBuildError."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        def mock_subprocess(cmd, **kwargs):
            # Image existence check - return not found to trigger build
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(returncode=1, stdout="", stderr="NOT_FOUND")
            # Submission fails with permission denied
            return MagicMock(returncode=1, stdout="", stderr="Permission denied")

        with patch("subprocess.run", side_effect=mock_subprocess):
            with pytest.raises(CloudBuildError) as exc_info:
                builder.build_image(
                    workspace_dir=workspace_dir,
                    workspace_name="test_ws",
                    version="v1",
                    backend="cloud",
                )

        assert "Permission denied" in str(exc_info.value)


class TestCloudBuildPolling:
    """Tests for Cloud Build status polling."""

    def test_waits_for_success_status(self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db):
        """Should poll until SUCCESS status."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        call_count = [0]

        def mock_subprocess(cmd, **kwargs):
            # Image existence check - return not found to trigger build
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(returncode=1, stdout="", stderr="NOT_FOUND")
            if "builds" in cmd and "submit" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"id": "build-123"}),
                    stderr="",
                )
            elif "builds" in cmd and "describe" in cmd:
                call_count[0] += 1
                if call_count[0] < 3:
                    # Return WORKING status for first two polls
                    return MagicMock(
                        returncode=0,
                        stdout=json.dumps({"status": "WORKING", "logUrl": "url"}),
                        stderr="",
                    )
                else:
                    # Return SUCCESS on third poll
                    return MagicMock(
                        returncode=0,
                        stdout=json.dumps({"status": "SUCCESS", "logUrl": "url"}),
                        stderr="",
                    )
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=mock_subprocess):
            with patch("time.sleep"):  # Don't actually sleep
                result = builder.build_image(
                    workspace_dir=workspace_dir,
                    workspace_name="test_ws",
                    version="v1",
                    backend="cloud",
                    wait=True,
                )

        assert call_count[0] == 3
        assert "goldfish-test_ws-v1" in result

    def test_failure_status_raises_error(self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db):
        """Should raise CloudBuildError on FAILURE status."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        def mock_subprocess(cmd, **kwargs):
            # Image existence check - return not found to trigger build
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(returncode=1, stdout="", stderr="NOT_FOUND")
            if "builds" in cmd and "submit" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"id": "build-123"}),
                    stderr="",
                )
            elif "builds" in cmd and "describe" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "status": "FAILURE",
                            "statusDetail": "Build failed: exit code 1",
                            "logUrl": "https://logs.url",
                        }
                    ),
                    stderr="",
                )
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=mock_subprocess):
            with pytest.raises(CloudBuildError) as exc_info:
                builder.build_image(
                    workspace_dir=workspace_dir,
                    workspace_name="test_ws",
                    version="v1",
                    backend="cloud",
                    wait=True,
                )

        # Error message should contain the failure detail and logs URL
        error_str = str(exc_info.value).lower()
        assert "exit code 1" in error_str or "failure" in error_str
        assert "logs" in error_str

    def test_timeout_raises_error(self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db):
        """Should raise CloudBuildError on timeout."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        # Set very short timeout for test (0.01 minutes = 0.6 seconds)
        mock_config_with_gce.docker.cloud_build.timeout_minutes = 0.01

        def mock_subprocess(cmd, **kwargs):
            # Image existence check - return not found to trigger build
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(returncode=1, stdout="", stderr="NOT_FOUND")
            if "builds" in cmd and "submit" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"id": "build-123"}),
                    stderr="",
                )
            elif "builds" in cmd and "describe" in cmd:
                # Always return WORKING - never complete
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"status": "WORKING", "logUrl": "url"}),
                    stderr="",
                )
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=mock_subprocess):
            with patch("goldfish.infra.docker_builder.time.sleep"):
                with pytest.raises(CloudBuildError) as exc_info:
                    builder.build_image(
                        workspace_dir=workspace_dir,
                        workspace_name="test_ws",
                        version="v1",
                        backend="cloud",
                        wait=True,
                    )

        assert "timed out" in str(exc_info.value).lower()


class TestCloudBuildDatabaseTracking:
    """Tests for database tracking of Cloud Build workspace builds."""

    def test_inserts_build_record_on_start(self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db):
        """Should insert docker_build record when starting Cloud Build."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        def mock_subprocess(cmd, **kwargs):
            # Image existence check - return not found to trigger build
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(returncode=1, stdout="", stderr="NOT_FOUND")
            if "builds" in cmd and "submit" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"id": "cloud-build-abc"}),
                    stderr="",
                )
            elif "builds" in cmd and "describe" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"status": "SUCCESS", "logUrl": "url"}),
                    stderr="",
                )
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=mock_subprocess):
            builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="my_workspace",
                version="v5",
                backend="cloud",
                wait=True,
            )

        # Verify insert was called with workspace info
        mock_db.insert_docker_build.assert_called_once()
        call_kwargs = mock_db.insert_docker_build.call_args[1]
        assert call_kwargs["workspace_name"] == "my_workspace"
        assert call_kwargs["version"] == "v5"
        assert call_kwargs["target"] == "workspace"
        assert call_kwargs["backend"] == "cloud"

    def test_updates_status_on_completion(self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db):
        """Should update docker_build status to completed on success."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        def mock_subprocess(cmd, **kwargs):
            # Image existence check - return not found to trigger build
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(returncode=1, stdout="", stderr="NOT_FOUND")
            if "builds" in cmd and "submit" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"id": "cloud-build-123"}),
                    stderr="",
                )
            elif "builds" in cmd and "describe" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"status": "SUCCESS", "logUrl": "https://logs.url"}),
                    stderr="",
                )
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=mock_subprocess):
            builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="test_ws",
                version="v1",
                backend="cloud",
                wait=True,
            )

        # Verify status update was called
        mock_db.update_docker_build_status.assert_called()
        final_call_kwargs = mock_db.update_docker_build_status.call_args[1]
        assert final_call_kwargs["status"] == "completed"
        assert "logs_uri" in final_call_kwargs

    def test_updates_status_on_failure(self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db):
        """Should update docker_build status to failed on error."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        def mock_subprocess(cmd, **kwargs):
            # Image existence check - return not found to trigger build
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(returncode=1, stdout="", stderr="NOT_FOUND")
            if "builds" in cmd and "submit" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"id": "cloud-build-123"}),
                    stderr="",
                )
            elif "builds" in cmd and "describe" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"status": "FAILURE", "logUrl": "url", "statusDetail": "OOM"}),
                    stderr="",
                )
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=mock_subprocess):
            with pytest.raises(CloudBuildError):
                builder.build_image(
                    workspace_dir=workspace_dir,
                    workspace_name="test_ws",
                    version="v1",
                    backend="cloud",
                    wait=True,
                )

        # Verify status update to failed was called
        mock_db.update_docker_build_status.assert_called()
        final_call_kwargs = mock_db.update_docker_build_status.call_args[1]
        assert final_call_kwargs["status"] == "failed"
        assert "error" in final_call_kwargs


class TestCloudBuildCaching:
    """Tests for cross-version caching in Cloud Build."""

    def test_uses_previous_version_for_cache(self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db):
        """Should use --cache-from with previous version image."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        # Mock a previous build existing for layer caching
        # Uses get_latest_docker_build_for_workspace (any version) for cache-from
        mock_db.get_latest_docker_build_for_workspace.return_value = {
            "registry_tag": "us-docker.pkg.dev/proj/goldfish/goldfish-test_ws-v4",
            "status": "completed",
        }

        captured_cloudbuild = {}

        def mock_subprocess(cmd, **kwargs):
            # Image existence check - return not found to trigger build
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(returncode=1, stdout="", stderr="NOT_FOUND")
            if "builds" in cmd and "submit" in cmd:
                for i, arg in enumerate(cmd):
                    if arg == "--config":
                        import yaml

                        config_path = Path(cmd[i + 1])
                        captured_cloudbuild["content"] = yaml.safe_load(config_path.read_text())
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"id": "build-123"}),
                    stderr="",
                )
            elif "builds" in cmd and "describe" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"status": "SUCCESS", "logUrl": "url"}),
                    stderr="",
                )
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=mock_subprocess):
            builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="test_ws",
                version="v5",
                backend="cloud",
                wait=True,
                use_cache=True,
            )

        # Check that cache-from was included in the build step
        assert "content" in captured_cloudbuild
        steps = captured_cloudbuild["content"]["steps"]

        # Should have a pull step for cache
        pull_steps = [s for s in steps if "pull" in str(s.get("args", []))]
        assert len(pull_steps) > 0

        # Build step should have --cache-from
        build_steps = [s for s in steps if "build" in str(s.get("args", []))]
        assert len(build_steps) > 0
        build_args = " ".join(str(a) for a in build_steps[0]["args"])
        assert "--cache-from" in build_args

    def test_skips_cache_when_use_cache_false(self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db):
        """Should use --no-cache when use_cache=False."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        captured_cloudbuild = {}

        def mock_subprocess(cmd, **kwargs):
            # Image existence check - return not found to trigger build
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(returncode=1, stdout="", stderr="NOT_FOUND")
            if "builds" in cmd and "submit" in cmd:
                for i, arg in enumerate(cmd):
                    if arg == "--config":
                        import yaml

                        config_path = Path(cmd[i + 1])
                        captured_cloudbuild["content"] = yaml.safe_load(config_path.read_text())
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"id": "build-123"}),
                    stderr="",
                )
            elif "builds" in cmd and "describe" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"status": "SUCCESS", "logUrl": "url"}),
                    stderr="",
                )
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=mock_subprocess):
            builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="test_ws",
                version="v1",
                backend="cloud",
                wait=True,
                use_cache=False,
            )

        # Check that --no-cache was used
        assert "content" in captured_cloudbuild
        steps = captured_cloudbuild["content"]["steps"]
        build_steps = [s for s in steps if "build" in str(s.get("args", []))]
        assert len(build_steps) > 0
        build_args = " ".join(str(a) for a in build_steps[0]["args"])
        assert "--no-cache" in build_args


class TestCloudBuildImageExistsCheck:
    """Tests for skipping builds based on content hash and registry state."""

    def test_skips_build_if_content_hash_matches_cached_build(
        self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig
    ):
        """Should skip Cloud Build if content hash matches a cached build."""
        # Mock database that returns a cached build matching any content hash
        mock_db = MagicMock()
        mock_db.get_docker_build_by_content_hash.return_value = {
            "registry_tag": "us-docker.pkg.dev/my-gcp-project/goldfish/goldfish-test_ws-v1",
        }

        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        call_log = []

        def mock_subprocess(cmd, **kwargs):
            call_log.append(cmd)
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=mock_subprocess):
            result = builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="test_ws",
                version="v1",
                backend="cloud",
                wait=True,
            )

        # Should return cached registry tag
        assert "goldfish-test_ws-v1" in result

        # Should NOT have called gcloud builds submit (skipped due to content hash match)
        submit_calls = [c for c in call_log if "builds" in c and "submit" in c]
        assert len(submit_calls) == 0, "Should not call builds submit when content hash matches"

        # Should NOT have called artifacts describe (skipped due to content hash match)
        describe_calls = [c for c in call_log if "artifacts" in c and "describe" in c]
        assert len(describe_calls) == 0, "Should skip registry check when content hash matches"

    def test_builds_if_image_does_not_exist(self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db):
        """Should proceed with Cloud Build if image does not exist."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        call_log = []

        def mock_subprocess(cmd, **kwargs):
            call_log.append(cmd)
            # Image does not exist - describe returns error
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(
                    returncode=1,
                    stdout="",
                    stderr="NOT_FOUND: image not found",
                )
            # Cloud Build submission and completion
            if "builds" in cmd and "submit" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"id": "cloud-build-123"}),
                    stderr="",
                )
            if "builds" in cmd and "describe" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"status": "SUCCESS", "logUrl": "url"}),
                    stderr="",
                )
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=mock_subprocess):
            result = builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="test_ws",
                version="v1",
                backend="cloud",
                wait=True,
            )

        # Should return registry tag
        assert "goldfish-test_ws-v1" in result

        # Should have called gcloud builds submit (did the build)
        submit_calls = [c for c in call_log if "builds" in c and "submit" in c]
        assert len(submit_calls) == 1, "Should call builds submit when image doesn't exist"

    def test_uses_hash_suffix_tag_when_image_exists_but_content_changed(
        self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig, mock_db
    ):
        """Should use tag with content hash suffix when image exists but content changed."""
        builder = DockerBuilder(config=mock_config_with_gce, db=mock_db)

        submitted_tag = []

        def mock_subprocess(cmd, **kwargs):
            # Image exists in registry (content changed scenario)
            if "artifacts" in cmd and "describe" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"digest": "sha256:abc"}),
                    stderr="",
                )
            # Capture the tag used in Cloud Build submission
            if "builds" in cmd and "submit" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"id": "cloud-build-123"}),
                    stderr="",
                )
            if "builds" in cmd and "describe" in cmd:
                return MagicMock(
                    returncode=0,
                    stdout=json.dumps({"status": "SUCCESS", "logUrl": "url"}),
                    stderr="",
                )
            return MagicMock(returncode=0)

        with patch("subprocess.run", side_effect=mock_subprocess):
            result = builder.build_image(
                workspace_dir=workspace_dir,
                workspace_name="my_workspace",
                version="v42",
                backend="cloud",
                wait=True,
            )

        # Tag should include content hash suffix since image exists with different content
        assert "goldfish-my_workspace-v42-" in result
        assert "us-docker.pkg.dev/my-gcp-project/goldfish" in result
        # Verify it's the hash suffix format (8 hex chars)
        parts = result.split("-")
        hash_suffix = parts[-1]
        assert len(hash_suffix) == 8
        assert all(c in "0123456789abcdef" for c in hash_suffix)


class TestGoldfishSourceCodeChanges:
    """Tests for detecting goldfish library code changes.

    When goldfish source code changes but workspace code doesn't, the Docker
    image should still be rebuilt because the content hash includes the goldfish
    code copied into the build context.
    """

    def test_different_goldfish_source_produces_different_content_hash(
        self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig
    ):
        """Content hash should change when goldfish source code changes."""
        from goldfish.infra.docker_builder import DockerBuilder

        builder = DockerBuilder(config=mock_config_with_gce)

        # Build two contexts with different goldfish code
        import tempfile

        with tempfile.TemporaryDirectory() as tmp1, tempfile.TemporaryDirectory() as tmp2:
            context1 = Path(tmp1)
            context2 = Path(tmp2)

            # Create minimal workspace structures
            for ctx in [context1, context2]:
                (ctx / "modules").mkdir()
                (ctx / "modules" / "train.py").write_text("# same code")
                (ctx / "configs").mkdir()
                (ctx / "configs" / "train.yaml").write_text("key: value")

            # Simulate goldfish code being copied
            goldfish1 = context1 / "goldfish_io" / "goldfish" / "io"
            goldfish1.mkdir(parents=True)
            (goldfish1 / "__init__.py").write_text("# version 1")

            goldfish2 = context2 / "goldfish_io" / "goldfish" / "io"
            goldfish2.mkdir(parents=True)
            (goldfish2 / "__init__.py").write_text("# version 2 - different!")

            # Generate same Dockerfile
            dockerfile = "FROM python:3.11-slim\nCOPY . /app"

            # Compute hashes
            hash1 = builder._compute_content_hash(context1, dockerfile, "python:3.11-slim")
            hash2 = builder._compute_content_hash(context2, dockerfile, "python:3.11-slim")

            # Hashes should be different
            assert hash1 != hash2, "Content hash should change when goldfish code changes"

    def test_same_goldfish_source_produces_same_content_hash(
        self, workspace_dir: Path, mock_config_with_gce: GoldfishConfig
    ):
        """Content hash should be stable when goldfish source code is unchanged."""
        from goldfish.infra.docker_builder import DockerBuilder

        builder = DockerBuilder(config=mock_config_with_gce)

        import tempfile

        with tempfile.TemporaryDirectory() as tmp1, tempfile.TemporaryDirectory() as tmp2:
            context1 = Path(tmp1)
            context2 = Path(tmp2)

            # Create identical workspace structures
            for ctx in [context1, context2]:
                (ctx / "modules").mkdir()
                (ctx / "modules" / "train.py").write_text("# same code")
                (ctx / "configs").mkdir()
                (ctx / "configs" / "train.yaml").write_text("key: value")

                # Same goldfish code
                goldfish = ctx / "goldfish_io" / "goldfish" / "io"
                goldfish.mkdir(parents=True)
                (goldfish / "__init__.py").write_text("# same goldfish code")

            # Generate same Dockerfile
            dockerfile = "FROM python:3.11-slim\nCOPY . /app"

            # Compute hashes
            hash1 = builder._compute_content_hash(context1, dockerfile, "python:3.11-slim")
            hash2 = builder._compute_content_hash(context2, dockerfile, "python:3.11-slim")

            # Hashes should be identical
            assert hash1 == hash2, "Content hash should be stable for identical content"

"""Unit tests for unified image versioning system.

TDD RED PHASE: These tests define the expected behavior for the refactored
image versioning system. They should FAIL with the current implementation
and PASS after the refactor is complete.

Key architecture (from design doc):
- New class: ImageVersionResolver in src/goldfish/cloud/image_versions.py
- New dataclass: ImageVersion with version, source, registry_tag fields
- Precedence: config -> DB -> default constant
- Supports both "base" and "project" image layers
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from goldfish.config import DockerConfig, GoldfishConfig


@pytest.fixture
def test_db(tmp_path: Path):
    """Create a test database."""
    from goldfish.db.database import Database

    db_path = tmp_path / ".goldfish" / "goldfish.db"
    return Database(db_path)


@pytest.fixture
def mock_config_no_version() -> GoldfishConfig:
    """Create a mock GoldfishConfig without base_image_version set."""
    config = MagicMock(spec=GoldfishConfig)
    config.project_name = "test-project"
    config.docker = DockerConfig()  # base_image_version defaults to None
    gce = MagicMock()
    gce.effective_artifact_registry = "us-docker.pkg.dev/my-project/goldfish"
    config.gce = gce
    return config


@pytest.fixture
def mock_config_with_version() -> GoldfishConfig:
    """Create a mock GoldfishConfig with base_image_version set."""
    config = MagicMock(spec=GoldfishConfig)
    config.project_name = "test-project"
    config.docker = DockerConfig(base_image_version="v99")
    gce = MagicMock()
    gce.effective_artifact_registry = "us-docker.pkg.dev/my-project/goldfish"
    config.gce = gce
    return config


# =============================================================================
# Tests for ImageVersionResolver class and ImageVersion dataclass
# =============================================================================


class TestImageVersionResolverImport:
    """Tests that the new module exists and exports expected symbols."""

    def test_can_import_resolver(self) -> None:
        """ImageVersionResolver should be importable."""
        from goldfish.cloud.image_versions import ImageVersionResolver

        assert ImageVersionResolver is not None

    def test_can_import_image_version_dataclass(self) -> None:
        """ImageVersion dataclass should be importable."""
        from goldfish.cloud.image_versions import ImageVersion

        assert ImageVersion is not None

    def test_can_import_default_constants(self) -> None:
        """Default version constants should be importable.

        Note: Only BASE images have defaults (Goldfish ships them).
        Project images do NOT have defaults - they're user-built.
        """
        from goldfish.cloud.image_versions import BASE_IMAGE_VERSION_DEFAULT

        # v10 is current default from profiles.py
        assert BASE_IMAGE_VERSION_DEFAULT == "v10"
        # PROJECT_IMAGE_VERSION_DEFAULT does NOT exist - project images are user-built


class TestImageVersionDataclass:
    """Tests for ImageVersion dataclass properties."""

    def test_image_version_has_version(self) -> None:
        """ImageVersion should have version attribute."""
        from goldfish.cloud.image_versions import ImageVersion

        iv = ImageVersion(version="v10", source="config", registry_tag=None)
        assert iv.version == "v10"

    def test_image_version_has_source(self) -> None:
        """ImageVersion should have source attribute."""
        from goldfish.cloud.image_versions import ImageVersion

        iv = ImageVersion(version="v10", source="database", registry_tag=None)
        assert iv.source == "database"

    def test_image_version_has_registry_tag(self) -> None:
        """ImageVersion should have registry_tag attribute."""
        from goldfish.cloud.image_versions import ImageVersion

        iv = ImageVersion(
            version="v10",
            source="database",
            registry_tag="us-docker.pkg.dev/proj/goldfish/goldfish-base-gpu:v10",
        )
        assert iv.registry_tag == "us-docker.pkg.dev/proj/goldfish/goldfish-base-gpu:v10"

    def test_registry_tag_can_be_none(self) -> None:
        """ImageVersion registry_tag should be optional (None for defaults)."""
        from goldfish.cloud.image_versions import ImageVersion

        iv = ImageVersion(version="v10", source="default", registry_tag=None)
        assert iv.registry_tag is None


class TestImageVersionResolverConstructor:
    """Tests for ImageVersionResolver initialization."""

    def test_constructor_accepts_config_and_db(self, mock_config_no_version: GoldfishConfig, test_db) -> None:
        """ImageVersionResolver should accept config and optional db."""
        from goldfish.cloud.image_versions import ImageVersionResolver

        resolver = ImageVersionResolver(mock_config_no_version, test_db)
        assert resolver is not None

    def test_constructor_accepts_config_only(self, mock_config_no_version: GoldfishConfig) -> None:
        """ImageVersionResolver should work without db."""
        from goldfish.cloud.image_versions import ImageVersionResolver

        resolver = ImageVersionResolver(mock_config_no_version, None)
        assert resolver is not None


# =============================================================================
# Tests for base image version resolution precedence
# =============================================================================


class TestBaseImageVersionPrecedence:
    """Tests for version resolution precedence: config -> DB -> default.

    These tests verify the core requirement that version resolution follows
    a clear priority order:
    1. Config override (goldfish.yaml docker.base_image_version)
    2. Database current version (base_image_versions table)
    3. Default constant (BASE_IMAGE_VERSION_DEFAULT)
    """

    def test_get_version_config_overrides_db(self, test_db, mock_config_with_version: GoldfishConfig) -> None:
        """Config base_image_version should take precedence over DB version.

        When both config and DB have a version, config should win.
        This allows users to pin a specific version in goldfish.yaml.
        """
        from goldfish.cloud.image_versions import ImageVersionResolver

        # Set a version in DB
        test_db.set_base_image_version("gpu", "v15", "us-docker.pkg.dev/my-project/goldfish/goldfish-base-gpu:v15")

        resolver = ImageVersionResolver(mock_config_with_version, test_db)

        # get_version should return config version (v99), not DB (v15)
        result = resolver.get_version(image_type="gpu", image_layer="base")
        assert result.version == "v99", (
            f"Expected config version 'v99' to override DB version 'v15', " f"but got '{result.version}'"
        )
        assert result.source == "config"

    def test_get_version_db_when_no_config(self, test_db, mock_config_no_version: GoldfishConfig) -> None:
        """DB version should be used when no config override is set.

        When config.docker.base_image_version is None, fall back to DB.
        """
        from goldfish.cloud.image_versions import ImageVersionResolver

        # Set a version in DB
        test_db.set_base_image_version("gpu", "v15", "us-docker.pkg.dev/my-project/goldfish/goldfish-base-gpu:v15")

        resolver = ImageVersionResolver(mock_config_no_version, test_db)

        # Should use v15 from DB
        result = resolver.get_version(image_type="gpu", image_layer="base")
        assert result.version == "v15", (
            f"Expected DB version 'v15' when no config override, " f"but got '{result.version}'"
        )
        assert result.source == "database"

    def test_get_version_default_fallback(self, test_db, mock_config_no_version: GoldfishConfig) -> None:
        """Default constant should be used when neither config nor DB has version.

        When config.docker.base_image_version is None AND DB has no version,
        fall back to the BASE_IMAGE_VERSION_DEFAULT constant.
        """
        from goldfish.cloud.image_versions import (
            BASE_IMAGE_VERSION_DEFAULT,
            ImageVersionResolver,
        )

        # Don't set any version in DB
        resolver = ImageVersionResolver(mock_config_no_version, test_db)

        # Should use default constant
        result = resolver.get_version(image_type="gpu", image_layer="base")
        assert result.version == BASE_IMAGE_VERSION_DEFAULT, (
            f"Expected default version '{BASE_IMAGE_VERSION_DEFAULT}' when no config or DB, "
            f"but got '{result.version}'"
        )
        assert result.source == "default"

    def test_get_version_default_when_no_db(self, mock_config_no_version: GoldfishConfig) -> None:
        """Default constant should be used when DB is not provided.

        Some code paths may not have access to DB. Should still work.
        """
        from goldfish.cloud.image_versions import (
            BASE_IMAGE_VERSION_DEFAULT,
            ImageVersionResolver,
        )

        # No DB at all
        resolver = ImageVersionResolver(mock_config_no_version, None)

        # Should use default constant
        result = resolver.get_version(image_type="gpu", image_layer="base")
        assert result.version == BASE_IMAGE_VERSION_DEFAULT, (
            f"Expected default version '{BASE_IMAGE_VERSION_DEFAULT}' when no DB, " f"but got '{result.version}'"
        )
        assert result.source == "default"


# =============================================================================
# Tests for different image types (CPU vs GPU)
# =============================================================================


class TestImageTypeResolution:
    """Tests for get_version with different image types.

    Verifies the method works correctly for both CPU and GPU image types.
    """

    def test_get_version_cpu_image_type(self, test_db, mock_config_no_version: GoldfishConfig) -> None:
        """get_version should work for CPU image type."""
        from goldfish.cloud.image_versions import ImageVersionResolver

        test_db.set_base_image_version("cpu", "v20", "us-docker.pkg.dev/my-project/goldfish/goldfish-base-cpu:v20")

        resolver = ImageVersionResolver(mock_config_no_version, test_db)

        result = resolver.get_version(image_type="cpu", image_layer="base")
        assert result.version == "v20"
        assert result.source == "database"

    def test_get_version_gpu_image_type(self, test_db, mock_config_no_version: GoldfishConfig) -> None:
        """get_version should work for GPU image type."""
        from goldfish.cloud.image_versions import ImageVersionResolver

        test_db.set_base_image_version("gpu", "v25", "us-docker.pkg.dev/my-project/goldfish/goldfish-base-gpu:v25")

        resolver = ImageVersionResolver(mock_config_no_version, test_db)

        result = resolver.get_version(image_type="gpu", image_layer="base")
        assert result.version == "v25"
        assert result.source == "database"

    def test_get_version_independent_types(self, test_db, mock_config_no_version: GoldfishConfig) -> None:
        """CPU and GPU versions should be resolved independently."""
        from goldfish.cloud.image_versions import ImageVersionResolver

        test_db.set_base_image_version("cpu", "v10", "us-docker.pkg.dev/my-project/goldfish/goldfish-base-cpu:v10")
        test_db.set_base_image_version("gpu", "v15", "us-docker.pkg.dev/my-project/goldfish/goldfish-base-gpu:v15")

        resolver = ImageVersionResolver(mock_config_no_version, test_db)

        assert resolver.get_version("cpu", "base").version == "v10"
        assert resolver.get_version("gpu", "base").version == "v15"


# =============================================================================
# Tests for image layers (base vs project)
# =============================================================================


class TestImageLayerResolution:
    """Tests for get_version with image_layer parameter.

    The system should support both "base" images (goldfish-base-*) and
    "project" images (project-specific images built on top of base).
    """

    def test_get_version_base_image_layer(self, test_db, mock_config_no_version: GoldfishConfig) -> None:
        """get_version should work for 'base' image layer."""
        from goldfish.cloud.image_versions import ImageVersionResolver

        test_db.set_base_image_version("gpu", "v12", "us-docker.pkg.dev/my-project/goldfish/goldfish-base-gpu:v12")

        resolver = ImageVersionResolver(mock_config_no_version, test_db)

        # Explicit base layer
        result = resolver.get_version(image_type="gpu", image_layer="base")
        assert result.version == "v12"

    def test_get_version_project_image_layer(self, test_db, mock_config_no_version: GoldfishConfig) -> None:
        """get_version should work for 'project' image layer.

        Project images have their own version tracking separate from base images.
        This test requires project_image_versions table and methods (Task #3/#5).
        """
        from goldfish.cloud.image_versions import ImageVersionResolver

        # Set project image version (DB method requires project_name)
        test_db.set_project_image_version(
            "test-project", "gpu", "v3", "us-docker.pkg.dev/my-project/goldfish/test-project-gpu:v3"
        )

        resolver = ImageVersionResolver(mock_config_no_version, test_db)

        # Project layer version
        result = resolver.get_version(image_type="gpu", image_layer="project")
        assert result.version == "v3"

    def test_get_version_project_returns_none_when_no_version(
        self, test_db, mock_config_no_version: GoldfishConfig
    ) -> None:
        """get_version for project layer returns None when no version exists.

        CRITICAL: Project images are user-built, not Goldfish-shipped.
        There is NO default project image version. If no config/DB version
        exists, the caller must decide: build a new image OR fall back to base.
        """
        from goldfish.cloud.image_versions import ImageVersionResolver

        # No project image version set in DB or config
        resolver = ImageVersionResolver(mock_config_no_version, test_db)

        # Should return None, NOT a default
        result = resolver.get_version(image_type="gpu", image_layer="project")
        assert result is None, (
            "Project images should return None when no version exists, "
            "NOT a hardcoded default. Project images are user-built."
        )

    def test_get_version_defaults_to_base_layer(self, test_db, mock_config_no_version: GoldfishConfig) -> None:
        """get_version should default to 'base' layer if not specified."""
        from goldfish.cloud.image_versions import ImageVersionResolver

        test_db.set_base_image_version("gpu", "v12", "us-docker.pkg.dev/my-project/goldfish/goldfish-base-gpu:v12")

        resolver = ImageVersionResolver(mock_config_no_version, test_db)

        # No layer specified - should default to base
        result = resolver.get_version(image_type="gpu")
        assert result.version == "v12"


# =============================================================================
# Tests for registry_tag in ImageVersion result
# =============================================================================


class TestRegistryTagGeneration:
    """Tests for registry_tag field in ImageVersion."""

    def test_get_version_includes_registry_tag(self, test_db, mock_config_no_version: GoldfishConfig) -> None:
        """ImageVersion should include full registry_tag when registry is configured."""
        from goldfish.cloud.image_versions import ImageVersionResolver

        test_db.set_base_image_version("gpu", "v15", "us-docker.pkg.dev/my-project/goldfish/goldfish-base-gpu:v15")

        resolver = ImageVersionResolver(mock_config_no_version, test_db)

        result = resolver.get_version("gpu", "base")
        assert result.registry_tag is not None
        assert "us-docker.pkg.dev/my-project/goldfish" in result.registry_tag
        assert ":v15" in result.registry_tag

    def test_get_version_registry_tag_uses_resolved_version(
        self, test_db, mock_config_with_version: GoldfishConfig
    ) -> None:
        """Registry tag should use the resolved version, not DB version."""
        from goldfish.cloud.image_versions import ImageVersionResolver

        # DB has v15, but config has v99
        test_db.set_base_image_version("gpu", "v15", "us-docker.pkg.dev/my-project/goldfish/goldfish-base-gpu:v15")

        resolver = ImageVersionResolver(mock_config_with_version, test_db)

        result = resolver.get_version("gpu", "base")
        # Should use config version v99 in registry tag
        assert ":v99" in result.registry_tag
        assert ":v15" not in result.registry_tag

    def test_get_version_registry_tag_none_when_no_registry(self, test_db) -> None:
        """Registry tag should be None when no artifact_registry configured."""
        from goldfish.cloud.image_versions import ImageVersionResolver

        # Config without artifact_registry
        config = MagicMock(spec=GoldfishConfig)
        config.project_name = "test-project"
        config.docker = DockerConfig()
        config.gce = None  # No GCE config

        resolver = ImageVersionResolver(config, test_db)

        result = resolver.get_version("gpu", "base")
        # No registry configured, so registry_tag should be None
        assert result.registry_tag is None


# =============================================================================
# Integration test: resolve_base_image still works
# =============================================================================


class TestResolveBaseImageIntegration:
    """Integration tests for resolve_base_image function.

    Verify existing resolve_base_image still works with explicit version param.
    """

    def test_resolve_base_image_with_registry_uses_version(self) -> None:
        """resolve_base_image function should accept version parameter.

        The standalone resolve_base_image function should support an
        explicit version override parameter.
        """
        from goldfish.cloud.adapters.gcp.profiles import resolve_base_image

        profile = {"base_image": "goldfish-base-gpu"}
        registry = "us-docker.pkg.dev/my-project/goldfish"

        # Pass explicit version
        result = resolve_base_image(profile, artifact_registry=registry, version="v50")

        assert ":v50" in result, f"Expected resolved image to use version 'v50', but got '{result}'"
        assert result == f"{registry}/goldfish-base-gpu:v50"


# =============================================================================
# Tests for DockerConfig schema updates
# =============================================================================


class TestDockerConfigProjectVersion:
    """Tests for DockerConfig.project_image_version field (new field)."""

    def test_docker_config_has_project_image_version_field(self) -> None:
        """DockerConfig should have project_image_version field."""
        config = DockerConfig(project_image_version="v5")
        assert config.project_image_version == "v5"

    def test_docker_config_project_image_version_defaults_to_none(self) -> None:
        """project_image_version should default to None."""
        config = DockerConfig()
        assert config.project_image_version is None

    def test_docker_config_both_versions_can_be_set(self) -> None:
        """Both base and project version overrides can be set simultaneously."""
        config = DockerConfig(
            base_image_version="v11",
            project_image_version="v3",
        )
        assert config.base_image_version == "v11"
        assert config.project_image_version == "v3"


# =============================================================================
# Tests for project_image_versions database table (Task #3/#5 dependency)
# =============================================================================


class TestProjectImageVersionsDatabase:
    """Tests for project_image_versions database table and methods.

    These tests define the expected behavior for the new database table
    that mirrors base_image_versions for project images.
    """

    def test_project_image_versions_table_exists(self, test_db) -> None:
        """project_image_versions table should exist after DB init."""
        with test_db._conn() as conn:
            result = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='project_image_versions'"
            ).fetchone()
        assert result is not None

    def test_set_project_image_version(self, test_db) -> None:
        """Should be able to set a project image version."""
        test_db.set_project_image_version("test-project", "gpu", "v1", "registry/test-project-gpu:v1")

        result = test_db.get_current_project_image_version("test-project", "gpu")
        assert result is not None
        assert result["version"] == "v1"
        assert result["image_type"] == "gpu"

    def test_set_project_image_version_updates_current(self, test_db) -> None:
        """Setting a new version should update the current version."""
        test_db.set_project_image_version("test-project", "gpu", "v1", "registry/test-project-gpu:v1")
        test_db.set_project_image_version("test-project", "gpu", "v2", "registry/test-project-gpu:v2")

        result = test_db.get_current_project_image_version("test-project", "gpu")
        assert result is not None
        assert result["version"] == "v2"

    def test_project_cpu_and_gpu_independent(self, test_db) -> None:
        """CPU and GPU project versions should be tracked independently."""
        test_db.set_project_image_version("test-project", "gpu", "v3", "registry/test-project-gpu:v3")
        test_db.set_project_image_version("test-project", "cpu", "v1", "registry/test-project-cpu:v1")

        gpu_result = test_db.get_current_project_image_version("test-project", "gpu")
        cpu_result = test_db.get_current_project_image_version("test-project", "cpu")

        assert gpu_result["version"] == "v3"
        assert cpu_result["version"] == "v1"

    def test_get_current_project_image_version_returns_none_when_empty(self, test_db) -> None:
        """Should return None for unknown image type."""
        result = test_db.get_current_project_image_version("test-project", "gpu")
        assert result is None

    def test_get_next_project_image_version_starts_at_v1(self, test_db) -> None:
        """First project version should be v1."""
        next_version = test_db.get_next_project_image_version("test-project", "gpu")
        assert next_version == "v1"

    def test_get_next_project_image_version_increments(self, test_db) -> None:
        """Should increment project version number correctly."""
        test_db.set_project_image_version("test-project", "gpu", "v5", "tag1")
        next_version = test_db.get_next_project_image_version("test-project", "gpu")
        assert next_version == "v6"


# =============================================================================
# Abstraction boundary tests - ensure provider isolation
# =============================================================================


class TestAbstractionBoundaries:
    """Tests that verify cloud provider isolation.

    Generic infrastructure code (infra/, jobs/) should NOT import directly
    from cloud/adapters/gcp/. Instead, they should use:
    - cloud/image_versions.py for image constants
    - cloud/protocols.py for backend interfaces
    - cloud/contracts.py for data types
    """

    def test_infra_base_images_imports_from_image_versions(self) -> None:
        """infra/base_images/__init__.py should import from image_versions, not gcp."""
        import ast
        from pathlib import Path

        init_file = Path("src/goldfish/infra/base_images/__init__.py")
        source = init_file.read_text()
        tree = ast.parse(source)

        gcp_imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module and "cloud.adapters.gcp" in node.module:
                    gcp_imports.append(node.module)

        assert not gcp_imports, (
            f"infra/base_images/__init__.py should not import from GCP-specific modules. "
            f"Found: {gcp_imports}. Use goldfish.cloud.image_versions instead."
        )

    def test_infra_base_images_manager_imports_from_image_versions(self) -> None:
        """infra/base_images/manager.py should import from image_versions, not gcp."""
        import ast
        from pathlib import Path

        manager_file = Path("src/goldfish/infra/base_images/manager.py")
        source = manager_file.read_text()
        tree = ast.parse(source)

        gcp_imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module and "cloud.adapters.gcp" in node.module:
                    gcp_imports.append(node.module)

        assert not gcp_imports, (
            f"infra/base_images/manager.py should not import from GCP-specific modules. "
            f"Found: {gcp_imports}. Use goldfish.cloud.image_versions instead."
        )

    def test_image_versions_has_no_project_default(self) -> None:
        """image_versions.py should NOT define PROJECT_IMAGE_VERSION_DEFAULT.

        Project images are user-built, not Goldfish-shipped. There is no sensible
        default version for project images.
        """
        import goldfish.cloud.image_versions as iv

        assert not hasattr(iv, "PROJECT_IMAGE_VERSION_DEFAULT"), (
            "image_versions.py should NOT have PROJECT_IMAGE_VERSION_DEFAULT. "
            "Project images are user-built, not Goldfish-shipped."
        )

    def test_image_versions_exports_all_base_constants(self) -> None:
        """image_versions.py should export all base image constants."""
        from goldfish.cloud.image_versions import (
            BASE_IMAGE_CPU,
            BASE_IMAGE_GPU,
            BASE_IMAGE_VERSION_DEFAULT,
            FALLBACK_BASE_IMAGE,
            PUBLIC_BASE_IMAGE_CPU,
            PUBLIC_BASE_IMAGE_GPU,
        )

        # Verify they're all strings
        assert isinstance(BASE_IMAGE_CPU, str)
        assert isinstance(BASE_IMAGE_GPU, str)
        assert isinstance(BASE_IMAGE_VERSION_DEFAULT, str)
        assert isinstance(FALLBACK_BASE_IMAGE, str)
        assert isinstance(PUBLIC_BASE_IMAGE_CPU, str)
        assert isinstance(PUBLIC_BASE_IMAGE_GPU, str)

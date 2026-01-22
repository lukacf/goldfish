"""Goldfish Daemon - Persistent background server.

This is the "singleton" server that:
- Owns the SQLite database
- Runs the pipeline worker thread
- Handles all tool operations
- Listens on a Unix Domain Socket for requests from MCP proxies

The MCP proxy (what Claude talks to) forwards requests here.
"""

import atexit
import fcntl
import json
import logging
import os
import signal
import socket
import socketserver
import stat
import sys
import threading
import time
from dataclasses import asdict, is_dataclass
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from socketserver import UnixStreamServer
from typing import Any

from pydantic import BaseModel

from goldfish.cloud.factory import AdapterFactory
from goldfish.config import GoldfishConfig
from goldfish.context import ServerContext, set_context
from goldfish.datasets.registry import DatasetRegistry
from goldfish.db.database import Database
from goldfish.errors import GoldfishError, ProjectNotInitializedError
from goldfish.infra.metadata.base import MetadataBus
from goldfish.infra.metadata.local import LocalMetadataBus
from goldfish.jobs.launcher import JobLauncher
from goldfish.jobs.pipeline_executor import PipelineExecutor
from goldfish.jobs.stage_executor import StageExecutor
from goldfish.jobs.tracker import JobTracker
from goldfish.logging import setup_logging
from goldfish.models import PipelineStatus
from goldfish.pipeline.manager import PipelineManager
from goldfish.state.state_md import StateManager
from goldfish.state_machine import transition
from goldfish.state_machine.exit_code import ExitCodeResult
from goldfish.state_machine.types import (
    EventContext,
    StageEvent,
    StageState,
    TerminationCause,
)
from goldfish.workspace.manager import WorkspaceManager

logger = logging.getLogger("goldfish.daemon")

# Protocol version for proxy/daemon compatibility
DAEMON_PROTOCOL_VERSION = "1.0"

# Maximum request body size (1MB)
MAX_REQUEST_BODY_SIZE = 1_000_000


def _get_version() -> str:
    """Get goldfish package version."""
    try:
        from importlib.metadata import version

        return version("goldfish")
    except Exception:
        return "unknown"


def _serialize_result(obj: Any) -> Any:
    """Serialize tool result to JSON-compatible format.

    Handles Pydantic models, dataclasses, datetime, etc.
    """
    if obj is None:
        return None
    if isinstance(obj, BaseModel):
        return obj.model_dump(mode="json")
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    if isinstance(obj, dict):
        return {k: _serialize_result(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_serialize_result(item) for item in obj]
    if hasattr(obj, "isoformat"):  # datetime, date, time
        return obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    # For primitive types, return as-is
    return obj


class ThreadedUnixHTTPServer(socketserver.ThreadingMixIn, UnixStreamServer):
    """Threaded HTTP server that listens on a Unix Domain Socket."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        server_address: str,
        RequestHandlerClass: type,
        lock_file: Path,
        bind_and_activate: bool = True,
        skip_lock: bool = False,
    ):
        self.lock_file = lock_file
        self.lock_fd: int | None = None
        self._skip_lock = skip_lock
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)

    def server_bind(self) -> None:
        """Bind to Unix socket with proper locking and permission handling."""
        socket_path = str(self.server_address)

        # Acquire exclusive lock to prevent race conditions
        self._acquire_lock()

        # Check if socket exists and is actually a socket
        if os.path.exists(socket_path):
            if not stat.S_ISSOCK(os.stat(socket_path).st_mode):
                raise RuntimeError(f"Path {socket_path} exists but is not a socket. " "Remove it manually if safe.")
            # Try to connect to existing socket
            try:
                test_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                test_sock.settimeout(1.0)
                test_sock.connect(socket_path)
                test_sock.close()
                # Socket is active - another daemon is running
                raise RuntimeError(f"Another daemon is already listening on {socket_path}")
            except (ConnectionRefusedError, OSError):
                # Socket is stale, safe to remove
                logger.debug("Removing stale socket: %s", socket_path)
                os.unlink(socket_path)

        super().server_bind()

        # Set restrictive permissions (owner only)
        os.chmod(socket_path, 0o600)

        # Ensure parent directory has proper permissions
        socket_dir = Path(socket_path).parent
        current_mode = socket_dir.stat().st_mode & 0o777
        if current_mode & 0o077:  # Group or world accessible
            logger.warning(
                "Socket directory %s has permissive mode %o, consider chmod 700",
                socket_dir,
                current_mode,
            )

    def _acquire_lock(self) -> None:
        """Acquire exclusive lock file to prevent multiple daemons.

        If skip_lock=True (set when startup lock already held), this is a no-op.
        """
        if self._skip_lock:
            # Lock already held from run_daemon() - don't re-acquire
            return

        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        self.lock_fd = os.open(str(self.lock_file), os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self.lock_fd)
            self.lock_fd = None
            raise RuntimeError("Another daemon is already running (lock file held)") from None

    def server_close(self) -> None:
        """Clean up socket and lock."""
        super().server_close()
        if self.lock_fd is not None:
            fcntl.flock(self.lock_fd, fcntl.LOCK_UN)
            os.close(self.lock_fd)
            self.lock_fd = None


class DaemonRequestHandler(BaseHTTPRequestHandler):
    """Handle HTTP requests from MCP proxy."""

    protocol_version = "HTTP/1.1"

    # Disable default logging to stderr
    def log_message(self, format: str, *args: Any) -> None:
        logger.debug("Request: %s", format % args)

    def _send_json_response(self, status_code: int, data: dict) -> None:
        """Send a JSON response with proper Content-Length."""
        payload = json.dumps(data).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        """Handle tool invocation requests."""
        # Validate path
        if self.path != "/tool":
            self._send_json_response(404, {"error": "Not found"})
            return

        # Check content length
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length > MAX_REQUEST_BODY_SIZE:
            self._send_json_response(413, {"error": f"Request too large (max {MAX_REQUEST_BODY_SIZE} bytes)"})
            return

        body = self.rfile.read(content_length)

        try:
            request = json.loads(body)
            tool_name = request.get("tool")
            params = request.get("params", {})

            logger.debug("Tool call: %s(%s)", tool_name, params)

            # Execute the tool
            result = self.server.daemon.execute_tool(tool_name, params)  # type: ignore[attr-defined]

            # Serialize result (handles Pydantic models, dataclasses, etc.)
            serialized = _serialize_result(result)

            self._send_json_response(200, {"result": serialized})

        except GoldfishError as e:
            self._send_json_response(
                400,
                {
                    "error": e.message,
                    "error_type": type(e).__name__,
                    "details": getattr(e, "details", None),
                },
            )
        except Exception as e:
            logger.exception("Tool execution failed: %s", e)
            self._send_json_response(500, {"error": str(e)})

    def do_GET(self) -> None:
        """Handle health check and status requests."""
        if self.path == "/health":
            daemon = self.server.daemon  # type: ignore[attr-defined]
            self._send_json_response(
                200,
                {
                    "status": "healthy",
                    "pid": os.getpid(),
                    "uptime": time.time() - daemon.start_time,
                    "project": str(daemon.project_root),
                    "version": _get_version(),
                    "protocol_version": DAEMON_PROTOCOL_VERSION,
                },
            )
        elif self.path == "/tools":
            daemon = self.server.daemon  # type: ignore[attr-defined]
            self._send_json_response(200, {"tools": list(daemon.tools.keys())})
        else:
            self._send_json_response(404, {"error": "Not found"})


class GoldfishDaemon:
    """The persistent Goldfish server daemon."""

    # How often to check for orphaned GCE instances (seconds)
    INSTANCE_MONITOR_INTERVAL = 60

    def __init__(self, project_root: Path):
        self.project_root = project_root.resolve()
        self.start_time = time.time()
        self.shutdown_event = threading.Event()
        self.tools: dict[str, Any] = {}
        self.context: ServerContext | None = None
        self.worker_thread: threading.Thread | None = None
        self.instance_monitor_thread: threading.Thread | None = None
        self.http_server: ThreadedUnixHTTPServer | None = None

        # Paths
        self.config: GoldfishConfig | None = None
        self.dev_repo_path: Path | None = None
        self.socket_path: Path | None = None
        self.pid_file: Path | None = None
        self.lock_file: Path | None = None
        self.log_file: Path | None = None

    def initialize(self) -> None:
        """Initialize the daemon - load config, connect to DB, register tools."""
        logger.info("Initializing daemon for project: %s", self.project_root)

        # Load config
        self.config = GoldfishConfig.load(self.project_root)
        self.dev_repo_path = self.config.get_dev_repo_path(self.project_root)

        # Set up paths in dev repo for database and logs
        goldfish_dir = self.dev_repo_path / ".goldfish"
        goldfish_dir.mkdir(parents=True, exist_ok=True)
        # Set restrictive permissions on .goldfish directory
        os.chmod(goldfish_dir, 0o700)

        # Socket/pid/lock files go in /tmp to avoid macOS path length limits
        self.socket_path = get_socket_path(self.project_root)
        self.pid_file = get_pid_file(self.project_root)
        self.lock_file = get_lock_file(self.project_root)
        # Log file stays in project for easy access
        self.log_file = goldfish_dir / "daemon.log"

        logger.debug("Socket path: %s", self.socket_path)
        logger.debug("PID file: %s", self.pid_file)
        logger.debug("Lock file: %s", self.lock_file)
        logger.debug("Log file: %s", self.log_file)

        # Initialize database with WAL mode for concurrent access
        db = Database(self.dev_repo_path / self.config.db_path)
        self._configure_db(db)

        # Initialize all components
        state_manager = StateManager(self.dev_repo_path / self.config.state_md.path, self.config)
        workspace_manager = WorkspaceManager(self.config, self.project_root, db, state_manager)
        job_launcher = JobLauncher(self.config, self.project_root, db, workspace_manager, state_manager)
        job_tracker = JobTracker(db, self.project_root)
        dataset_registry = DatasetRegistry(db, self.config)
        pipeline_manager = PipelineManager(db, workspace_manager, dataset_registry=dataset_registry)
        stage_executor = StageExecutor(
            db=db,
            config=self.config,
            workspace_manager=workspace_manager,
            pipeline_manager=pipeline_manager,
            project_root=self.project_root,
            dataset_registry=dataset_registry,
        )
        pipeline_executor = PipelineExecutor(stage_executor=stage_executor, pipeline_manager=pipeline_manager, db=db)

        # Initialize MetadataBus using AdapterFactory (Cloud-native or Local simulation)
        # The factory creates the appropriate adapter based on config.jobs.backend
        adapter_factory = AdapterFactory(self.config)
        local_metadata_bus: LocalMetadataBus | None = None

        # Create signal bus (MetadataBus) via factory
        signal_bus = adapter_factory.create_signal_bus(metadata_path=self.dev_repo_path / ".metadata_bus.json")

        # For backwards compatibility, cast to MetadataBus
        # Both LocalMetadataBus and GCPSignalBus implement the SignalBus protocol
        metadata_bus: MetadataBus
        if isinstance(signal_bus, LocalMetadataBus):
            local_metadata_bus = signal_bus
            metadata_bus = signal_bus
        else:
            # For GCP backend, we need the old-style MetadataBus interface
            # The GCPSignalBus adapter wraps GCPMetadataBus
            from goldfish.infra.metadata.gcp import GCPMetadataBus

            metadata_bus = GCPMetadataBus()

        # Create and set context
        self.context = ServerContext(
            project_root=self.project_root,
            config=self.config,
            db=db,
            workspace_manager=workspace_manager,
            state_manager=state_manager,
            job_launcher=job_launcher,
            job_tracker=job_tracker,
            pipeline_manager=pipeline_manager,
            dataset_registry=dataset_registry,
            stage_executor=stage_executor,
            pipeline_executor=pipeline_executor,
            metadata_bus=metadata_bus,
        )
        set_context(self.context)

        # Start local metadata syncer for Overdrive parity in non-GCE environments
        if local_metadata_bus is not None:
            from goldfish.infra.metadata.local_syncer import LocalMetadataSyncer

            self._local_metadata_syncer = LocalMetadataSyncer(
                bus=local_metadata_bus,
                stage_executor=stage_executor,
            )
            self._local_metadata_syncer.start()

        # Store references for worker
        self._db = db
        self._pipeline_executor = pipeline_executor
        self._pipeline_manager = pipeline_manager
        self._stage_executor = stage_executor

        # Register all tools
        self._register_tools()

        logger.info("Daemon initialized successfully")

    def _configure_db(self, db: Database) -> None:
        """Configure database for concurrent access."""
        logger.debug("Configuring database for concurrent access")
        with db._conn() as conn:
            # Enable WAL mode for better concurrent read/write
            result = conn.execute("PRAGMA journal_mode=WAL").fetchone()
            logger.debug("Database journal_mode set to: %s", result[0] if result else "unknown")
            # Set busy timeout to wait up to 30 seconds for locks
            conn.execute("PRAGMA busy_timeout=30000")
            logger.debug("Database busy_timeout set to 30000ms")
            # Ensure synchronous is at least NORMAL for durability
            conn.execute("PRAGMA synchronous=NORMAL")
            logger.debug("Database synchronous mode set to NORMAL")

    def _register_tools(self) -> None:
        """Register all tool handlers."""
        # Import tools modules - they register with the context
        # We'll build a dispatch table from the MCP tool registry
        from goldfish.server import mcp

        # Get all registered tools from FastMCP
        for tool in mcp._tool_manager._tools.values():
            self.tools[tool.name] = tool.fn
            logger.debug("Registered tool: %s", tool.name)

        logger.info("Registered %d tools", len(self.tools))

    def execute_tool(self, tool_name: str, params: dict) -> Any:
        """Execute a tool by name with given parameters."""
        if tool_name not in self.tools:
            logger.warning("Unknown tool requested: %s", tool_name)
            raise GoldfishError(f"Unknown tool: {tool_name}")

        logger.debug("Executing tool: %s", tool_name)
        start_time = time.time()
        try:
            fn = self.tools[tool_name]
            result = fn(**params)
            elapsed = time.time() - start_time
            logger.debug("Tool %s completed in %.3fs", tool_name, elapsed)
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.debug("Tool %s failed after %.3fs: %s", tool_name, elapsed, e)
            raise

    def start_worker(self) -> None:
        """Start the pipeline worker thread using PipelineExecutor directly."""

        def worker_loop() -> None:
            logger.info("Worker thread starting")
            poll_interval = 2.0

            while not self.shutdown_event.is_set():
                try:
                    self._poll_and_process_pipelines()
                except Exception as e:
                    logger.exception("Worker error: %s", e)

                # Sleep with shutdown check
                self.shutdown_event.wait(timeout=poll_interval)

            logger.info("Worker thread stopped")

        self.worker_thread = threading.Thread(target=worker_loop, daemon=True, name="pipeline-worker")
        self.worker_thread.start()

    def _poll_and_process_pipelines(self) -> None:
        """Poll for pending pipelines and process them.

        Uses PipelineExecutor's existing queue processing logic.
        """
        # Find pipelines that need processing
        with self._db._conn() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT pr.id, pr.workspace_name, pr.pipeline_name,
                       pr.config_override, pr.inputs_override
                FROM pipeline_runs pr
                JOIN pipeline_stage_queue psq ON psq.pipeline_run_id = pr.id
                WHERE pr.status IN (?, ?)
                AND psq.status IN (?, ?)
                ORDER BY pr.started_at DESC
                LIMIT 10
                """,
                (PipelineStatus.PENDING, PipelineStatus.RUNNING, PipelineStatus.PENDING, PipelineStatus.RUNNING),
            ).fetchall()

        for row in rows:
            if self.shutdown_event.is_set():
                break

            pipeline_run_id = row["id"]
            workspace = row["workspace_name"]
            pipeline_name = row["pipeline_name"]
            # Load persisted overrides
            config_override = json.loads(row["config_override"]) if row["config_override"] else None
            inputs_override = json.loads(row["inputs_override"]) if row["inputs_override"] else None

            try:
                # Check if pipeline is done
                pending, running = self._pipeline_executor._pipeline_queue_counts(pipeline_run_id)
                if pending == 0 and running == 0:
                    self._pipeline_executor._finalize_pipeline_run(pipeline_run_id)
                    logger.info("Pipeline %s completed", pipeline_run_id)
                    continue

                # Log queue state for debugging stalls
                logger.debug(
                    "Pipeline %s [%s]: pending=%d, running=%d",
                    pipeline_run_id,
                    workspace,
                    pending,
                    running,
                )

                # Process one round of the queue
                launched = self._pipeline_executor._process_pipeline_queue_once(
                    pipeline_run_id=pipeline_run_id,
                    workspace=workspace,
                    pipeline_name=pipeline_name,
                    config_override=config_override,
                    inputs_override=inputs_override,
                    reason="Worker processing",
                )
                if launched:
                    logger.info(
                        "Pipeline %s: launched %d stage(s) [%s]",
                        pipeline_run_id,
                        len(launched),
                        ", ".join(s.stage for s in launched),
                    )
                elif pending > 0 and running == 0:
                    # Nothing launched but stages are pending - they must be waiting on deps
                    logger.debug(
                        "Pipeline %s: %d stage(s) waiting (deps not ready or blocked)",
                        pipeline_run_id,
                        pending,
                    )
            except Exception as e:
                logger.exception("Error processing pipeline %s: %s", pipeline_run_id, e)

    def start_instance_monitor(self) -> None:
        """Start the GCE instance monitor thread (Cost Protection Layer 3).

        This thread periodically checks for orphaned GCE instances and cleans them up.
        It catches instances that slipped through the startup script's self-deletion.
        """

        def monitor_loop() -> None:
            logger.info("Instance monitor thread starting")

            # Wait a bit before first check to let things settle
            self.shutdown_event.wait(timeout=30)

            # State-machine poller (single source of truth for stage run lifecycle)
            from goldfish.state_machine.stage_daemon import StageDaemon

            stage_daemon = StageDaemon(db=self._db, config=self.config)

            while not self.shutdown_event.is_set():
                try:
                    # NOTE: Legacy orphan checker is deprecated; state machine daemon drives state.
                    stage_daemon.poll_active_runs()
                    self._cleanup_stalled_pipelines()
                except Exception as e:
                    logger.exception("Instance monitor error: %s", e)

                self.shutdown_event.wait(timeout=self.INSTANCE_MONITOR_INTERVAL)

            logger.info("Instance monitor thread stopped")

        self.instance_monitor_thread = threading.Thread(target=monitor_loop, daemon=True, name="instance-monitor")
        self.instance_monitor_thread.start()

    def _cleanup_stalled_pipelines(self) -> None:
        """Mark old running/pending pipelines as failed if they haven't progressed.

        This prevents worker queue starvation by clearing 'zombie' runs that
        stalled due to worker crashes, restarts, or unhandled errors.
        """
        # Stale threshold: 4 hours (generous for long-running experiments)
        threshold = "4 hours"
        with self._db._conn() as conn:
            # 1. Clear stalled queue entries
            conn.execute(
                f"""
                UPDATE pipeline_stage_queue
                SET status = ?, error = ?
                WHERE status IN (?, ?)
                AND (claimed_at < datetime('now', '-{threshold}') OR (claimed_at IS NULL AND id IN (
                    SELECT psq.id FROM pipeline_stage_queue psq
                    JOIN pipeline_runs pr ON pr.id = psq.pipeline_run_id
                    WHERE pr.started_at < datetime('now', '-{threshold}')
                )))
                """,
                (PipelineStatus.FAILED, "Stalled (auto-cleanup)", PipelineStatus.PENDING, PipelineStatus.RUNNING),
            )

            # 2. Clear stalled pipeline runs
            conn.execute(
                f"""
                UPDATE pipeline_runs
                SET status = ?, error = ?
                WHERE status IN (?, ?)
                AND started_at < datetime('now', '-{threshold}')
                """,
                (PipelineStatus.FAILED, "Stalled (auto-cleanup)", PipelineStatus.PENDING, PipelineStatus.RUNNING),
            )

    def _check_if_preempted(self, instance_name: str, project_id: str) -> bool:
        """Check if a GCE instance was preempted.

        Queries the compute operations API to see if there's a preemption event
        for this instance. Parses targetLink to find exact instance name match.

        Args:
            instance_name: The GCE instance name (e.g., stage-abc123)
            project_id: The GCP project ID

        Returns:
            True if the instance was preempted, False otherwise
        """
        import subprocess

        try:
            # Query recent preemption operations, then parse to find exact match
            # Use 7-day window (preemptions are recent events, no need to search older)
            # Limit 100 is generous (typical projects have <10 spot instances)
            result = subprocess.run(
                [
                    "gcloud",
                    "compute",
                    "operations",
                    "list",
                    f"--project={project_id}",
                    "--filter=operationType=compute.instances.preempted AND insertTime>-P7D",
                    "--limit=100",
                    "--format=value(targetLink)",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )

            # Parse targetLinks to find exact instance match
            # targetLink format: .../projects/{project}/zones/{zone}/instances/{name}
            target_links: list[str] = []
            if result.stdout.strip():
                # Filter out empty lines from gcloud output
                target_links = [link for link in result.stdout.strip().split("\n") if link]
                logger.debug(
                    "Found %d preemption operations in last 7 days, checking for %s",
                    len(target_links),
                    instance_name,
                )
                # Pre-compute expected suffix for efficiency
                expected_suffix = f"/instances/{instance_name}"
                for target_link in target_links:
                    # Check if targetLink ends with our instance name
                    if target_link.endswith(expected_suffix):
                        logger.info(
                            "Confirmed preemption for %s (targetLink: %s)",
                            instance_name,
                            target_link,
                        )
                        return True

            logger.debug(
                "No preemption found for %s (checked %d operations)",
                instance_name,
                len(target_links),
            )
            return False
        except Exception as e:
            logger.debug("Failed to check preemption status for %s: %s", instance_name, e)
            return False

    def _check_orphaned_instances(self) -> None:
        """Check for and clean up orphaned GCE instances.

        Two-way orphan detection:
        1. DB says 'running' but instance gone → mark run as failed
        2. Instance running but DB says terminal (canceled/completed/failed) → delete instance

        This catches both directions of inconsistency.
        """
        import subprocess

        if not self.config:
            return

        # Get GCE project ID - CRITICAL: must match where instances are created
        if not self.config.gce:
            logger.debug("No GCE config, skipping orphan check")
            return

        try:
            project_id = self.config.gce.effective_project_id
        except ValueError:
            logger.debug("No GCE project_id configured, skipping orphan check")
            return

        # Get list of ALL GCE stage instances (any status) in one call
        # We need to know status to avoid killing instances that are still booting
        try:
            result = subprocess.run(
                [
                    "gcloud",
                    "compute",
                    "instances",
                    "list",
                    f"--project={project_id}",
                    "--filter=name~^stage-",
                    "--format=value(name,zone,status)",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                logger.warning(
                    "Failed to list GCE instances (rc=%s): %s",
                    result.returncode,
                    (result.stderr or "").strip(),
                )
                return
            # Track all instances with their status
            # alive_instances: instances that exist and are not terminated/stopped
            # Key is instance name, value is (zone, status)
            all_instances: dict[str, tuple[str, str]] = {}  # name -> (zone, status)
            if result.stdout.strip():
                for line in result.stdout.strip().split("\n"):
                    parts = line.split()
                    if len(parts) >= 3:
                        name = parts[0]
                        zone = parts[1].split("/")[-1]  # Extract zone from full path
                        status = parts[2]
                        all_instances[name] = (zone, status)

            # Instances are "alive" if they're in any non-terminal state
            # PROVISIONING, STAGING, RUNNING, STOPPING, SUSPENDING, SUSPENDED are all "alive"
            # Only TERMINATED is truly dead
            terminal_statuses = {"TERMINATED", "STOPPED"}
            alive_instances = {
                name: zone for name, (zone, status) in all_instances.items() if status not in terminal_statuses
            }
        except Exception as e:
            logger.warning("Failed to list GCE instances: %s", e)
            return

        # === Check 1: DB says active but instance is gone ===
        # IMPORTANT: Add grace period - don't check runs started less than 20 minutes ago
        # H100/GPU instance allocation can take 10+ minutes due to capacity constraints
        # Use state (source of truth) instead of legacy status column (v1.2: FINALIZING → POST_RUN)
        # Note: AWAITING_USER_FINALIZATION is NOT included - instance is already cleaned up by then
        active_states = (
            StageState.RUNNING.value,
            StageState.POST_RUN.value,
        )
        placeholders = ", ".join("?" for _ in active_states)
        with self._db._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id, backend_handle, workspace_name, stage_name, state
                FROM stage_runs
                WHERE state IN ({placeholders})
                AND backend_type = 'gce'
                AND backend_handle IS NOT NULL
                AND started_at < datetime('now', '-20 minutes')
                """,
                active_states,
            ).fetchall()

        if rows:
            logger.debug("Checking %d running GCE stage runs", len(rows))

        for row in rows:
            stage_run_id = row["id"]
            backend_handle = row["backend_handle"]
            workspace = row["workspace_name"]
            stage = row["stage_name"]

            # Sanitize instance name (same logic as GCELauncher)
            instance_name = backend_handle.replace("_", "-").lower()[:60]

            if instance_name not in alive_instances:
                # Check for exit_code.txt in GCS first to handle race conditions
                # where the instance self-deletes before final state update.
                exit_result = self._get_exit_code(stage_run_id)

                if exit_result.exists and exit_result.code == 0 and not exit_result.gcs_error:
                    # Instance completed successfully - use EXIT_SUCCESS event
                    logger.info(
                        "Stage run %s completed successfully (found exit_code=0 in GCS)",
                        stage_run_id,
                    )
                    try:
                        from datetime import UTC, datetime

                        transition(
                            self._db,
                            stage_run_id,
                            StageEvent.EXIT_SUCCESS,
                            EventContext(
                                timestamp=datetime.now(UTC),
                                source="daemon",
                            ),
                        )
                        logger.info(
                            "Marked stage run %s as completed (workspace=%s, stage=%s)",
                            stage_run_id,
                            workspace,
                            stage,
                        )
                    except Exception as e:
                        logger.exception("Failed to transition stage run %s: %s", stage_run_id, e)
                else:
                    # Instance disappeared - use INSTANCE_LOST event
                    # Check for termination cause file first (supervisor/watchdog kills write this)
                    explicit_cause = self._get_termination_cause(stage_run_id)
                    was_preempted = self._check_if_preempted(instance_name, project_id)
                    exit_code_val = exit_result.code if exit_result.exists else None

                    if explicit_cause is not None:
                        # Supervisor or watchdog killed it - use that cause
                        term_cause = explicit_cause
                        error_msg = f"Instance terminated by {explicit_cause.value} (exit_code={exit_code_val})"
                        logger.warning(
                            "%s terminated stage run: %s (instance %s, cause=%s)",
                            explicit_cause.value.title(),
                            stage_run_id,
                            instance_name,
                            explicit_cause.value,
                        )
                    elif was_preempted:
                        error_msg = "Instance preempted by GCE (spot/preemptible)"
                        term_cause = TerminationCause.PREEMPTED
                        logger.warning(
                            "Preempted stage run detected: %s (instance %s was preempted)",
                            stage_run_id,
                            instance_name,
                        )
                    else:
                        error_msg = f"Instance disappeared (orphan cleanup, exit_code={exit_code_val})"
                        term_cause = TerminationCause.ORPHANED
                        logger.warning(
                            "Orphaned stage run detected: %s (instance %s not running)",
                            stage_run_id,
                            instance_name,
                        )

                    try:
                        from datetime import UTC, datetime

                        transition(
                            self._db,
                            stage_run_id,
                            StageEvent.INSTANCE_LOST,
                            EventContext(
                                timestamp=datetime.now(UTC),
                                source="daemon",
                                termination_cause=term_cause,
                                error_message=error_msg,
                            ),
                        )
                        logger.info(
                            "Marked stage run %s as terminated (workspace=%s, stage=%s, cause=%s)",
                            stage_run_id,
                            workspace,
                            stage,
                            term_cause.value,
                        )
                    except Exception as e:
                        logger.exception("Failed to transition stage run %s: %s", stage_run_id, e)

    def _get_exit_code(self, stage_run_id: str, max_attempts: int = 2, retry_delay: float = 1.0) -> ExitCodeResult:
        """Get exit code from GCS for a stage run.

        Args:
            stage_run_id: Stage run identifier
            max_attempts: Number of retry attempts
            retry_delay: Seconds between retries

        Returns:
            ExitCodeResult with proper categorization (exists, code, gcs_error).
        """
        from goldfish.state_machine.exit_code import get_exit_code_gce

        if not self.config or not self.config.gce or not self.config.gcs or not self.config.gcs.bucket:
            # No bucket configured - assume success for local-like behavior
            return ExitCodeResult.from_code(0)

        bucket = self.config.gcs.bucket
        bucket_uri = bucket if bucket.startswith("gs://") else f"gs://{bucket}"

        try:
            project_id = self.config.gce.effective_project_id if self.config.gce else None
            return get_exit_code_gce(
                bucket_uri=bucket_uri,
                stage_run_id=stage_run_id,
                project_id=project_id,
                max_attempts=max_attempts,
                retry_delay=retry_delay,
            )
        except Exception as e:
            # Defensive: treat unexpected errors as GCS errors
            return ExitCodeResult.from_gcs_error(str(e))

    def _get_termination_cause(self, stage_run_id: str) -> TerminationCause | None:
        """Get termination cause from GCS for a stage run.

        The startup script writes termination_cause.txt when supervisor
        or watchdog kills the job, before the instance is deleted.

        Args:
            stage_run_id: Stage run identifier

        Returns:
            TerminationCause if file exists and is valid, None otherwise.
        """
        import subprocess

        if not self.config or not self.config.gce or not self.config.gcs or not self.config.gcs.bucket:
            return None

        bucket = self.config.gcs.bucket
        bucket_uri = bucket if bucket.startswith("gs://") else f"gs://{bucket}"
        gcs_path = f"{bucket_uri}/runs/{stage_run_id}/logs/termination_cause.txt"

        try:
            project_id = self.config.gce.effective_project_id if self.config.gce else None
            cmd = ["gsutil"]
            if project_id:
                cmd.extend(["-o", f"GSUtil:project_id={project_id}"])
            cmd.extend(["cat", gcs_path])

            result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=10)
            cause_str = result.stdout.strip().lower()

            # Map string to enum (file contains internal names for simplicity)
            cause_map = {
                "supervisor": TerminationCause.NO_HEARTBEAT,
                "watchdog": TerminationCause.MAX_RUNTIME_EXCEEDED,
            }
            return cause_map.get(cause_str)

        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            # File not found or timeout - not an error, just no cause info
            return None
        except Exception as e:
            logger.debug("Error reading termination cause for %s: %s", stage_run_id, e)
            return None

    def start_http_server(self) -> None:
        """Start the HTTP server on Unix socket."""
        if not self.socket_path or not self.lock_file:
            raise RuntimeError("Daemon not initialized")

        logger.debug("Creating HTTP server on Unix socket: %s", self.socket_path)
        logger.debug("Using lock file: %s", self.lock_file)

        # Create threaded server - skip lock acquisition since we hold startup lock
        self.http_server = ThreadedUnixHTTPServer(
            str(self.socket_path), DaemonRequestHandler, self.lock_file, skip_lock=True
        )
        self.http_server.daemon = self  # type: ignore[attr-defined]

        logger.info("HTTP server listening on %s", self.socket_path)

    def write_pid_file(self) -> None:
        """Write PID file and project_root file atomically."""
        if self.pid_file:
            logger.debug("Writing PID file: %s (pid=%d)", self.pid_file, os.getpid())
            # Write to temp file first, then rename (atomic on POSIX)
            temp_file = self.pid_file.with_suffix(".tmp")
            temp_file.write_text(str(os.getpid()))
            temp_file.rename(self.pid_file)
            atexit.register(lambda: self.pid_file.unlink(missing_ok=True) if self.pid_file else None)
            logger.debug("PID file written successfully")

            # Write project_root file for web server discovery
            project_root_file = self.pid_file.parent / "project_root"
            temp_root_file = project_root_file.with_suffix(".tmp")
            temp_root_file.write_text(str(self.project_root))
            temp_root_file.rename(project_root_file)
            atexit.register(lambda: project_root_file.unlink(missing_ok=True))
            logger.debug("Project root file written for web server discovery")

    def _maybe_start_web_server(self) -> None:
        """Auto-start the web visualization server.

        Starts automatically unless GOLDFISH_NO_WEB=1 is set.
        Skips if web server is already running.
        """
        # Check if auto-start is disabled
        no_web = os.environ.get("GOLDFISH_NO_WEB", "").lower() in ("1", "true", "yes")
        if no_web:
            logger.debug("Web server auto-start disabled via GOLDFISH_NO_WEB")
            return

        try:
            # Import here to avoid circular dependency
            from goldfish.web_server import is_web_server_running, spawn_web_server

            # Check if web server is already running
            running, pid, port = is_web_server_running()
            if running:
                logger.info("Web server already running (pid=%d, port=%d)", pid, port)
                return

            # Spawn web server
            logger.info("Auto-starting web visualization server...")
            pid = spawn_web_server(open_browser=False)  # Don't open browser on daemon start
            logger.info("Web server started (pid=%d)", pid)
            logger.info("Visit http://127.0.0.1:7342 to view provenance")

        except Exception as e:
            # Don't fail daemon startup if web server fails
            logger.warning("Failed to auto-start web server: %s", e)

    def run(self) -> None:
        """Run the daemon main loop."""
        self.write_pid_file()
        self.start_worker()
        self.start_instance_monitor()  # Cost Protection Layer 3
        self.start_http_server()

        # Optionally auto-start web server for visualization
        self._maybe_start_web_server()

        # Set up signal handlers - shutdown from a different thread to avoid deadlock
        def handle_shutdown(signum: int, frame: Any) -> None:
            logger.info("Received signal %d, shutting down...", signum)
            # Start shutdown in a new thread to avoid deadlock
            # (signal handler runs in main thread which is blocked in serve_forever)
            threading.Thread(target=self.shutdown, daemon=True).start()

        signal.signal(signal.SIGTERM, handle_shutdown)
        signal.signal(signal.SIGINT, handle_shutdown)

        logger.info(
            "Daemon running (pid=%d, version=%s, protocol=%s)",
            os.getpid(),
            _get_version(),
            DAEMON_PROTOCOL_VERSION,
        )

        # Serve forever
        try:
            if self.http_server:
                self.http_server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            # Only call shutdown if not already shutting down
            if not self.shutdown_event.is_set():
                self.shutdown()

    def shutdown(self) -> None:
        """Gracefully shutdown the daemon."""
        if self.shutdown_event.is_set():
            return  # Already shutting down

        logger.info("Shutting down daemon...")
        self.shutdown_event.set()

        if self.http_server:
            self.http_server.shutdown()
            self.http_server.server_close()

        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5.0)

        if self.instance_monitor_thread and self.instance_monitor_thread.is_alive():
            self.instance_monitor_thread.join(timeout=5.0)

        # Clean up PID file (socket cleanup handled by server_close)
        if self.pid_file and self.pid_file.exists():
            self.pid_file.unlink(missing_ok=True)

        logger.info("Daemon stopped")


def _get_socket_dir(project_root: Path) -> Path:
    """Get the directory for daemon socket/pid/lock files.

    Uses ~/.goldfish/sockets/<hash>/ because:
    - macOS has a 104-byte limit on Unix socket paths
    - Project paths in iCloud or deep directories can easily exceed this
    - User's home directory is stable and user-specific

    Returns:
        Path to socket directory (e.g., ~/.goldfish/sockets/abc123def456)
    """
    import hashlib

    # Create a short, unique directory name based on project path hash
    path_hash = hashlib.sha256(str(project_root.resolve()).encode()).hexdigest()[:12]
    socket_dir = Path.home() / ".goldfish" / "sockets" / path_hash
    socket_dir.mkdir(parents=True, exist_ok=True)

    # Set restrictive permissions
    os.chmod(socket_dir, 0o700)

    return socket_dir


def get_socket_path(project_root: Path) -> Path:
    """Get the socket path for a project."""
    return _get_socket_dir(project_root) / "goldfish.sock"


def get_pid_file(project_root: Path) -> Path:
    """Get the PID file path for a project."""
    return _get_socket_dir(project_root) / "daemon.pid"


def get_lock_file(project_root: Path) -> Path:
    """Get the lock file path for a project."""
    return _get_socket_dir(project_root) / "daemon.lock"


def is_daemon_running(project_root: Path) -> tuple[bool, int | None]:
    """Check if daemon is running for a project.

    Uses multiple checks for robustness:
    1. PID file exists and process is alive
    2. Socket exists and responds to health check

    Returns:
        Tuple of (is_running, pid)
    """
    try:
        pid_file = get_pid_file(project_root)
        socket_path = get_socket_path(project_root)

        # Check PID file
        if not pid_file.exists():
            return False, None

        pid = int(pid_file.read_text().strip())

        # Check if process is alive
        try:
            os.kill(pid, 0)
        except (ProcessLookupError, PermissionError):
            return False, None

        # Verify socket is responsive (handles PID reuse)
        if socket_path.exists():
            try:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(2.0)
                sock.connect(str(socket_path))
                # Send minimal health check
                request = b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n"
                sock.sendall(request)
                response = sock.recv(1024)
                sock.close()
                if b"200 OK" in response and b"healthy" in response:
                    return True, pid
            except (ConnectionRefusedError, OSError, TimeoutError):
                pass

        return False, None

    except (ValueError, ProjectNotInitializedError):
        return False, None


def spawn_daemon(project_root: Path) -> int:
    """Spawn the daemon as a detached background process.

    Returns:
        PID of spawned daemon
    """
    import subprocess

    logger.debug("Spawning daemon for project: %s", project_root)

    # Get log file path for daemon output
    try:
        config = GoldfishConfig.load(project_root)
        dev_repo_path = config.get_dev_repo_path(project_root)
        log_file = dev_repo_path / ".goldfish" / "daemon.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        log_file = project_root / ".goldfish" / "daemon.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

    logger.debug("Daemon log file: %s", log_file)

    cmd = [
        sys.executable,
        "-m",
        "goldfish",
        "daemon",
        "--project",
        str(project_root),
    ]

    logger.debug("Daemon command: %s", " ".join(cmd))

    # Spawn detached with log file for debugging
    with open(log_file, "a") as log_fd:
        proc = subprocess.Popen(
            cmd,
            start_new_session=True,
            stdin=subprocess.DEVNULL,
            stdout=log_fd,
            stderr=log_fd,
        )

    logger.debug("Daemon process spawned with pid=%d", proc.pid)

    # Give it a moment to start
    time.sleep(0.5)

    return proc.pid


def stop_daemon(project_root: Path, timeout: float = 10.0) -> bool:
    """Stop the daemon and wait for it to exit.

    Args:
        project_root: Project root directory
        timeout: Maximum seconds to wait for daemon to exit

    Returns:
        True if daemon stopped successfully, False if timeout
    """
    logger.debug("Stopping daemon for project: %s", project_root)

    running, pid = is_daemon_running(project_root)
    if not running or pid is None:
        logger.debug("Daemon not running, nothing to stop")
        return True

    logger.debug("Sending SIGTERM to daemon pid=%d", pid)
    # Send SIGTERM
    os.kill(pid, signal.SIGTERM)

    # Wait for exit
    start = time.time()
    while time.time() - start < timeout:
        try:
            os.kill(pid, 0)
            time.sleep(0.1)
        except ProcessLookupError:
            elapsed = time.time() - start
            logger.debug("Daemon stopped after %.2fs", elapsed)
            return True

    logger.warning("Daemon did not stop within %.1fs timeout", timeout)
    return False


def run_daemon(project_root: Path) -> None:
    """Entry point for running the daemon."""
    # Acquire lock IMMEDIATELY to prevent multiple daemons from starting
    # This must happen before logging or any other initialization
    lock_file = get_lock_file(project_root)
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = os.open(str(lock_file), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        # Another daemon is starting or running - exit silently
        os.close(lock_fd)
        sys.exit(0)

    # Now we have the lock - proceed with initialization
    setup_logging(component="daemon")

    logger.info("Starting Goldfish daemon for %s", project_root)

    daemon = GoldfishDaemon(project_root)
    # Transfer lock ownership to daemon
    daemon._startup_lock_fd = lock_fd  # type: ignore[attr-defined]

    try:
        daemon.initialize()
        daemon.run()
    except ProjectNotInitializedError as e:
        logger.error("Project not initialized: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.exception("Daemon failed: %s", e)
        sys.exit(1)
    finally:
        # Release startup lock
        if lock_fd is not None:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
            except OSError:
                pass

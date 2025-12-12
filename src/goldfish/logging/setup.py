"""Logging system setup using VictoriaLogs via non-blocking queue handler."""

import atexit
import logging
import logging.handlers
import os
import queue
import sys
import uuid
from pathlib import Path

from .settings import get_settings

# Global instance ID - set once during logging setup
_INSTANCE_ID: str | None = None

# Track if logging has been set up
_LOGGING_INITIALIZED = False


def generate_instance_id() -> str:
    """Generate semantic instance ID based on runtime context."""
    project = Path.cwd().name  # Project name from current directory
    is_container = Path("/.dockerenv").exists()  # Docker container detection
    context = "container" if is_container else "host"
    session = str(uuid.uuid4())[:8]  # Unique session identifier

    return f"goldfish_{project}_{context}_{session}"


def get_instance_id() -> str | None:
    """Get the current instance ID.

    Returns None if logging hasn't been set up yet.
    """
    return _INSTANCE_ID


def setup_logging(component: str = "server") -> None:
    """Initialize VictoriaLogs-based logging.

    Args:
        component: Component name (server, worker, stage) for log tagging
    """
    global _LOGGING_INITIALIZED, _INSTANCE_ID

    # Avoid double initialization
    if _LOGGING_INITIALIZED:
        return

    settings = get_settings()

    # Get the root logger for the goldfish package
    app_logger = logging.getLogger("goldfish")
    app_logger.setLevel(getattr(logging, settings.logging.level, logging.INFO))
    app_logger.propagate = False

    if app_logger.hasHandlers():
        app_logger.handlers.clear()

    # Check if VictoriaLogs is disabled
    if not settings.logging.victoria_logs_enabled:
        # Add stderr handler so we can still see messages
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.INFO)
        stderr_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        app_logger.addHandler(stderr_handler)
        app_logger.info("VictoriaLogs DISABLED - logging to stderr only")
        _LOGGING_INITIALIZED = True
        return

    # Generate instance ID once for this session
    _INSTANCE_ID = generate_instance_id()
    instance_id = _INSTANCE_ID

    # Set up non-blocking VictoriaLogs handler using queue with timeout protection
    victoria_logs_url = settings.logging.victoria_logs_url

    try:
        from .handlers import TimeoutLokiHandler

        # Create a queue for non-blocking logging
        log_queue: queue.Queue = queue.Queue(-1)  # -1 means unlimited size

        loki_handler = TimeoutLokiHandler(
            url=f"{victoria_logs_url}/insert/loki/api/v1/push?_stream_fields=app,instance_id,component",
            tags={
                "app": settings.logging.loki_app_tag,
                "instance_id": instance_id,
                "component": component,
                "project": settings.logging.project_path or os.getcwd(),
            },
            version="1",
            timeout=10.0,
        )
        loki_handler.setLevel(getattr(logging, settings.logging.level, logging.INFO))

        # Create the queue handler
        queue_handler = logging.handlers.QueueHandler(log_queue)

        # Create and start the queue listener
        queue_listener = logging.handlers.QueueListener(log_queue, loki_handler, respect_handler_level=True)
        queue_listener.start()

        # Store listener for cleanup
        app_logger._queue_listener = queue_listener  # type: ignore[attr-defined]

        # Register cleanup on exit
        atexit.register(queue_listener.stop)

        app_logger.addHandler(queue_handler)

        app_logger.info(
            "VictoriaLogs logging initialized [instance=%s, component=%s, url=%s]",
            instance_id,
            component,
            victoria_logs_url,
        )
    except ImportError:
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.INFO)
        stderr_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        app_logger.addHandler(stderr_handler)
        app_logger.warning(
            "logging_loki not installed - falling back to stderr. " "Install with: pip install python-logging-loki"
        )
    except Exception as e:
        # Don't block startup if VictoriaLogs is unavailable
        print(f"Warning: Could not set up VictoriaLogs: {e}", file=sys.stderr)
        # Fall back to stderr
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.INFO)
        stderr_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        app_logger.addHandler(stderr_handler)

    _LOGGING_INITIALIZED = True


def shutdown_logging() -> None:
    """Stop the queue listener if it exists."""
    global _LOGGING_INITIALIZED
    app_logger = logging.getLogger("goldfish")
    if hasattr(app_logger, "_queue_listener"):
        listener = app_logger._queue_listener
        # Only stop if thread is still alive (avoids double-stop from atexit)
        if hasattr(listener, "_thread") and listener._thread is not None:
            listener.stop()
        delattr(app_logger, "_queue_listener")
    _LOGGING_INITIALIZED = False

"""Custom logging handlers with timeout support."""

import os
import sys
import traceback

import requests
from logging_loki import LokiHandler

from .settings import get_settings


class TimeoutLokiHandler(LokiHandler):
    """Custom LokiHandler that enforces a network timeout on all requests.

    This prevents the logging thread from hanging indefinitely on stale connections.
    """

    def __init__(self, *args, timeout: float = 10.0, **kwargs):
        """Initialize handler with configurable timeout.

        Args:
            timeout: Request timeout in seconds (default: 10.0)
            *args, **kwargs: Passed to parent LokiHandler
        """
        # Store the URL for later checking
        self.handler_url = args[0] if args else kwargs.get("url", "")
        super().__init__(*args, **kwargs)
        self.timeout = timeout

        # Override the emitter's session to use our timeout
        if hasattr(self, "emitter") and hasattr(self.emitter, "_session"):
            self.emitter._session = None

    def emit(self, record):
        """Emit a record with timeout protection."""
        settings = get_settings()

        # In E2E mode, skip localhost connections
        if settings.dev.ci_e2e and "localhost:9428" in self.handler_url:
            return

        try:
            if hasattr(self, "build_msg"):
                self.build_msg(record)
            else:
                self.format(record)

            session = self.emitter.session

            # Install timeout adapter once
            if not hasattr(session, "_timeout_adapter_installed"):
                adapter = requests.adapters.HTTPAdapter()
                adapter.max_retries = 0
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                session._timeout_adapter_installed = True

            # Patch post to include timeout
            original_post = session.post

            def post_with_timeout(*args, **kwargs):
                kwargs["timeout"] = self.timeout
                return original_post(*args, **kwargs)

            session.post = post_with_timeout
            super().emit(record)

        except requests.exceptions.Timeout:
            print(
                f"LokiHandler timeout after {self.timeout}s - connection may be stale",
                file=sys.stderr,
            )
            if hasattr(self.emitter, "close"):
                self.emitter.close()
        except requests.exceptions.RequestException as e:
            self._handle_request_error(e)
        except Exception:
            if not self._is_suppressed_error():
                self.handleError(record)

    def _handle_request_error(self, e: Exception) -> None:
        """Handle request exceptions with E2E suppression."""
        settings = get_settings()
        error_str = str(e)
        is_localhost_error = "localhost" in error_str and "9428" in error_str
        is_e2e_test = settings.dev.ci_e2e
        victoria_url = settings.logging.victoria_logs_url
        has_proper_url = victoria_url and "host.docker.internal" in victoria_url

        if not (is_localhost_error and is_e2e_test and has_proper_url):
            print(f"LokiHandler network error: {e}", file=sys.stderr)

        if hasattr(self.emitter, "close"):
            self.emitter.close()

    def _is_suppressed_error(self) -> bool:
        """Check if current exception should be suppressed."""
        exc_info = sys.exc_info()
        if exc_info[0] is None:
            return False

        exc_text = "".join(traceback.format_exception(*exc_info))
        is_localhost_error = "localhost" in exc_text and "9428" in exc_text
        is_e2e_test = os.getenv("CI_E2E") == "1"

        return is_localhost_error and is_e2e_test

    def handleError(self, record):
        """Override to suppress localhost connection errors in E2E tests."""
        if self._is_suppressed_error():
            return
        super().handleError(record)

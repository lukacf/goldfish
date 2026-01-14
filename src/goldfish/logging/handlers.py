"""Custom logging handlers with timeout support."""

import sys

import requests
from logging_loki import LokiHandler

from .settings import get_settings


class TimeoutLokiHandler(LokiHandler):
    """Custom LokiHandler that enforces a network timeout on all requests.

    This prevents the logging thread from hanging indefinitely on stale connections.
    Fails gracefully when VictoriaLogs is unavailable - never crashes the server.
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
        self._disabled = False
        self._error_logged = False

        # Override the emitter's session to use our timeout
        if hasattr(self, "emitter") and hasattr(self.emitter, "_session"):
            self.emitter._session = None

    def emit(self, record):
        """Emit a record with timeout protection.

        Fails gracefully - if VictoriaLogs is unavailable, logs are silently
        dropped after a single warning message. This ensures the MCP server
        never crashes due to an optional logging backend being unavailable.
        """
        # If handler has been disabled due to errors, silently drop logs
        if self._disabled:
            return

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
            self._handle_connection_failure(f"VictoriaLogs timeout after {self.timeout}s - disabling remote logging")
        except requests.exceptions.RequestException as e:
            self._handle_connection_failure(f"VictoriaLogs unavailable ({type(e).__name__}) - disabling remote logging")
        except Exception as e:
            # Catch ALL exceptions - logging should NEVER crash the server
            self._handle_connection_failure(f"VictoriaLogs error ({type(e).__name__}: {e}) - disabling remote logging")

    def _handle_connection_failure(self, message: str) -> None:
        """Handle connection failures gracefully.

        Logs a warning once and disables the handler to prevent log spam.
        """
        if not self._error_logged:
            print(f"Warning: {message}", file=sys.stderr)
            self._error_logged = True

        # Disable handler after first failure to avoid repeated errors
        self._disabled = True

        # Clean up emitter resources
        if hasattr(self.emitter, "close"):
            try:
                self.emitter.close()
            except Exception:
                pass  # Ignore cleanup errors

    def handleError(self, record):
        """Override to never print ugly tracebacks.

        VictoriaLogs is optional - errors should be handled gracefully,
        not with verbose tracebacks that confuse users.
        """
        # Silently handle all errors - we've already logged a warning
        # in _handle_connection_failure if needed
        pass

"""Entry point for: python -m goldfish"""

import argparse
import os
import sys
from pathlib import Path


def _discover_project(explicit_path: Path | None) -> Path:
    """Discover project root from explicit path or auto-discovery."""
    if explicit_path:
        return explicit_path.resolve()

    # Check GOLDFISH_START_DIR or use CWD
    start_dir_str = os.environ.get("GOLDFISH_START_DIR")
    start_dir = Path(start_dir_str) if start_dir_str else Path.cwd()

    # Check current directory
    if (start_dir / "goldfish.yaml").exists():
        return start_dir

    # Search parent directories (up to 10 levels)
    current = start_dir
    for _ in range(10):
        if current.parent == current:
            break
        current = current.parent
        if (current / "goldfish.yaml").exists():
            return current

    # Search immediate subdirectories
    try:
        for subdir in start_dir.iterdir():
            if subdir.is_dir() and (subdir / "goldfish.yaml").exists():
                return subdir
    except (PermissionError, OSError):
        pass

    # Fall back to CWD
    return start_dir


def main():
    parser = argparse.ArgumentParser(description="Goldfish - MCP server for ML experimentation")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # serve command (default) - runs MCP proxy that connects to daemon
    serve_parser = subparsers.add_parser("serve", help="Run MCP server (connects to daemon)")
    serve_parser.add_argument(
        "--project",
        "-p",
        type=Path,
        default=None,
        help="Project root directory (default: auto-discover)",
    )

    # daemon command - runs the persistent background server
    daemon_parser = subparsers.add_parser("daemon", help="Run persistent daemon server (internal use)")
    daemon_parser.add_argument(
        "--project",
        "-p",
        type=Path,
        required=True,
        help="Project root directory",
    )

    # init command
    init_parser = subparsers.add_parser("init", help="Initialize a new project")
    init_parser.add_argument("name", help="Project name")
    init_parser.add_argument(
        "--path",
        type=Path,
        default=None,
        help="Project root path (default: ./<name>)",
    )
    init_parser.add_argument(
        "--from",
        dest="source",
        type=Path,
        default=None,
        help="Import existing code from this directory",
    )

    # status command - check daemon status
    status_parser = subparsers.add_parser("status", help="Check daemon status")
    status_parser.add_argument(
        "--project",
        "-p",
        type=Path,
        default=None,
        help="Project root directory",
    )

    # stop command - stop daemon
    stop_parser = subparsers.add_parser("stop", help="Stop daemon")
    stop_parser.add_argument(
        "--project",
        "-p",
        type=Path,
        default=None,
        help="Project root directory",
    )

    args = parser.parse_args()

    if args.command == "init":
        _handle_init(args)

    elif args.command == "serve" or args.command is None:
        project_root = _discover_project(getattr(args, "project", None))

        from goldfish.mcp_proxy import run_proxy

        run_proxy(project_root)

    elif args.command == "daemon":
        # Run the daemon directly (usually spawned by proxy)
        from goldfish.daemon import run_daemon

        run_daemon(args.project)

    elif args.command == "status":
        _handle_status(args)

    elif args.command == "stop":
        _handle_stop(args)

    else:
        parser.print_help()
        sys.exit(1)


def _handle_init(args):
    """Handle init command."""
    project_path = args.path or Path.cwd() / args.name

    if args.source:
        from goldfish.init import init_from_existing

        config = init_from_existing(project_path, args.source)
        print(f"Initialized '{args.name}' with code from {args.source}")
    else:
        from goldfish.init import init_project

        config = init_project(args.name, project_path)
        print(f"Initialized '{args.name}'")

    print(f"  Project: {project_path}")
    print(f"  Dev repo: {project_path.parent / config.dev_repo_path}")
    print(f"  Config: {project_path / 'goldfish.yaml'}")
    print(f"  State: {project_path / config.state_md.path}")
    print()
    print("Next steps:")
    print("  1. Edit goldfish.yaml to configure your project")
    print("  2. The daemon will start automatically when Claude connects")


def _handle_status(args):
    """Handle status command."""
    project_root = _discover_project(args.project)

    try:
        from goldfish.daemon import is_daemon_running

        running, pid = is_daemon_running(project_root)

        if running:
            print(f"Daemon is running (pid={pid})")
            print(f"Project: {project_root}")
        else:
            print("Daemon is not running")
            print(f"Project: {project_root}")

    except Exception as e:
        print(f"Error checking status: {e}")
        sys.exit(1)


def _handle_stop(args):
    """Handle stop command."""
    project_root = _discover_project(args.project)

    try:
        from goldfish.daemon import is_daemon_running, stop_daemon

        running, pid = is_daemon_running(project_root)

        if not running:
            print("Daemon is not running")
            return

        print(f"Stopping daemon (pid={pid})...")
        if stop_daemon(project_root, timeout=10.0):
            print("Daemon stopped")
        else:
            print("Warning: Daemon did not stop within timeout")
            sys.exit(1)

    except Exception as e:
        print(f"Error stopping daemon: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

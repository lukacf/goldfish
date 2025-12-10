"""Entry point for: python -m goldfish"""

import argparse
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Goldfish - MCP server for ML experimentation")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # serve command (default)
    serve_parser = subparsers.add_parser("serve", help="Run MCP server")
    serve_parser.add_argument(
        "--project",
        "-p",
        type=Path,
        default=None,
        help="Project root directory (default: auto-discover)",
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

    args = parser.parse_args()

    if args.command == "init":
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
        print("  1. Edit goldfish.yaml to add invariants")
        print("  2. Run: python -m goldfish serve --project", project_path)

    elif args.command == "serve" or args.command is None:
        # Default to serve if no command given
        # Use project arg if provided, otherwise auto-discover or use CWD

        # DEBUG: Log what's happening
        import sys

        debug_msg = f"[DEBUG] serve command: args.command={args.command}, hasattr={hasattr(args, 'project')}, args.project={getattr(args, 'project', 'N/A')}"
        print(debug_msg, file=sys.stderr)
        try:
            with open("/tmp/goldfish_main_debug.log", "a") as f:
                f.write(f"{debug_msg}\n")
        except OSError:
            pass

        if hasattr(args, "project") and args.project:
            try:
                with open("/tmp/goldfish_main_debug.log", "a") as f:
                    f.write(f"[DEBUG] Taking IF branch: args.project={args.project}\n")
            except OSError:
                pass
            project_root = args.project
        else:
            try:
                with open("/tmp/goldfish_main_debug.log", "a") as f:
                    f.write("[DEBUG] Taking ELSE branch (autodiscovery)\n")
            except OSError:
                pass
            # Auto-discover: search for goldfish.yaml
            import os
            import sys

            project_root = None
            # Check if GOLDFISH_START_DIR is set (for cases where uv changes CWD)
            start_dir_str = os.environ.get("GOLDFISH_START_DIR")
            if start_dir_str:
                start_dir = Path(start_dir_str)
            else:
                start_dir = Path.cwd()
            try:
                with open("/tmp/goldfish_main_debug.log", "a") as f:
                    f.write(f"[DEBUG] GOLDFISH_START_DIR={start_dir_str}, start_dir = {start_dir}\n")
            except OSError:
                pass

            # Debug logging to file (in addition to stderr)
            debug_log = start_dir / "goldfish_autodiscovery.log"

            def log(msg):
                try:
                    with open(debug_log, "a") as f:
                        f.write(f"{msg}\n")
                except OSError:
                    pass
                print(msg, file=sys.stderr)

            log(f"[GOLDFISH AUTO-DISCOVERY] Starting from: {start_dir}")

            # First, check current directory
            if (start_dir / "goldfish.yaml").exists():
                project_root = start_dir
                log(f"[GOLDFISH AUTO-DISCOVERY] Found in current dir: {project_root}")

            # If not found, search parent directories (up to 10 levels up)
            if project_root is None:
                current = start_dir
                for _ in range(10):
                    if current.parent == current:  # Reached filesystem root
                        break
                    current = current.parent
                    if (current / "goldfish.yaml").exists():
                        project_root = current
                        log(f"[GOLDFISH AUTO-DISCOVERY] Found in parent: {project_root}")
                        break

            # If still not found, search immediate subdirectories (1 level down)
            if project_root is None:
                log(f"[GOLDFISH AUTO-DISCOVERY] Searching subdirectories of: {start_dir}")
                try:
                    for subdir in start_dir.iterdir():
                        log(f"[GOLDFISH AUTO-DISCOVERY] Checking subdir: {subdir}")
                        if subdir.is_dir() and (subdir / "goldfish.yaml").exists():
                            project_root = subdir
                            log(f"[GOLDFISH AUTO-DISCOVERY] Found in subdir: {project_root}")
                            break
                except (PermissionError, OSError) as e:
                    log(f"[GOLDFISH AUTO-DISCOVERY] Error scanning subdirs: {e}")
                    pass  # Ignore errors when scanning subdirectories

            # Fall back to CWD if no project found
            if project_root is None:
                project_root = start_dir
                log(f"[GOLDFISH AUTO-DISCOVERY] No project found, using CWD: {project_root}")

            log(f"[GOLDFISH AUTO-DISCOVERY] Final project_root: {project_root}")

        from goldfish.server import run_server

        run_server(project_root)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()

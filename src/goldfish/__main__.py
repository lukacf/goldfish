"""Entry point for: python -m goldfish"""

import argparse
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description="Goldfish - MCP server for ML experimentation"
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # serve command (default)
    serve_parser = subparsers.add_parser("serve", help="Run MCP server")
    serve_parser.add_argument(
        "--project",
        "-p",
        type=Path,
        default=Path.cwd(),
        help="Project root directory (default: current directory)",
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
        # Use project arg if provided, otherwise use CWD
        if hasattr(args, 'project') and args.project:
            project_root = args.project
        else:
            project_root = Path.cwd()

        from goldfish.server import run_server

        run_server(project_root)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()

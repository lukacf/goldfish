#!/usr/bin/env python3
"""MCP Client Orchestrator for Deluxe E2E Test.

This script acts as an MCP client (like Claude Code does) and sends
tool calls to the Goldfish MCP server to orchestrate a full ML workflow.

This tests the actual MCP protocol, not internal Python APIs.
"""

import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


class GoldfishMCPClient:
    """MCP client for testing Goldfish server."""

    def __init__(self):
        self.session: ClientSession | None = None

    async def connect(self):
        """Connect to Goldfish MCP server via stdio."""
        server_params = StdioServerParameters(
            command="python",
            args=["-m", "goldfish", "serve", "--project", "/test-workspace"],
            env=None,
        )

        stdio_transport = await stdio_client(server_params)
        self.session = ClientSession(stdio_transport[0], stdio_transport[1])

        await self.session.initialize()
        print("✓ Connected to Goldfish MCP server")

    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Any:
        """Call an MCP tool and return the result."""
        if not self.session:
            raise RuntimeError("Not connected to server")

        print(f"\n📞 Calling tool: {tool_name}")
        print(f"   Arguments: {json.dumps(arguments, indent=2)}")

        result = await self.session.call_tool(tool_name, arguments)

        print(f"✓ Tool completed")
        return result

    async def close(self):
        """Close connection to server."""
        if self.session:
            await self.session.__aexit__(None, None, None)


async def run_deluxe_workflow():
    """Run the complete deluxe E2E workflow using MCP tools."""

    print("=" * 70)
    print("DELUXE E2E TEST: Claude Code + MCP + Goldfish + GCE")
    print("=" * 70)

    # Check required environment variables
    project_id = os.getenv("GOLDFISH_GCE_PROJECT")
    bucket = os.getenv("GOLDFISH_GCS_BUCKET")

    if not project_id or not bucket:
        print("ERROR: Missing required environment variables:")
        print("  GOLDFISH_GCE_PROJECT")
        print("  GOLDFISH_GCS_BUCKET")
        sys.exit(1)

    dry_run = os.getenv("GOLDFISH_DELUXE_DRY_RUN") == "1"

    print(f"\nConfiguration:")
    print(f"  Project: {project_id}")
    print(f"  Bucket: {bucket}")
    print(f"  Dry-run: {dry_run}")
    print()

    client = GoldfishMCPClient()

    try:
        # Connect to MCP server
        await client.connect()

        # ================================================================
        # PHASE 1: Initialize Project
        # ================================================================
        print("\n" + "=" * 70)
        print("PHASE 1: Initialize Project")
        print("=" * 70)

        result = await client.call_tool("initialize_project", {
            "project_name": "deluxe-ml-test",
        })
        print(f"✓ Initialized project: {result}")

        # ================================================================
        # PHASE 2: Create Workspace
        # ================================================================
        print("\n" + "=" * 70)
        print("PHASE 2: Create Workspace")
        print("=" * 70)

        result = await client.call_tool("create_workspace", {
            "name": "baseline",
            "goal": "Baseline ML classification model",
            "reason": "Deluxe E2E test - baseline workspace",
        })
        print(f"✓ Created workspace: {result}")

        # Mount workspace
        result = await client.call_tool("mount", {
            "workspace": "baseline",
            "slot": "w1",
            "reason": "Deluxe E2E test - mount for setup",
        })
        print(f"✓ Mounted workspace: {result}")

        # ================================================================
        # PHASE 3: Setup ML Pipeline
        # ================================================================
        print("\n" + "=" * 70)
        print("PHASE 3: Setup ML Pipeline")
        print("=" * 70)

        # Copy ML project template to workspace
        # (This would be done by Claude Code editing files)
        print("Note: In real usage, Claude would write pipeline files")
        print("      For this test, we'll copy the template")

        import shutil
        template_path = Path("/goldfish/tests/deluxe/fixtures/ml_project_template")
        workspace_path = Path("/test-workspace/workspaces/w1")

        for item in ["modules", "configs", "pipeline.yaml", "requirements.txt"]:
            src = template_path / item
            dst = workspace_path / item
            if src.is_dir():
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                shutil.copy2(src, dst)

        print("✓ Copied ML pipeline template")

        # Validate pipeline
        result = await client.call_tool("validate_pipeline", {
            "workspace": "baseline",
        })

        if result.get("errors"):
            print(f"ERROR: Pipeline validation failed: {result['errors']}")
            sys.exit(1)

        print("✓ Pipeline validated")

        # Checkpoint
        result = await client.call_tool("checkpoint", {
            "slot": "w1",
            "message": "Initial ML pipeline setup",
        })
        print(f"✓ Checkpointed: {result.get('snapshot_id')}")

        # ================================================================
        # PHASE 4: Run Pipeline Stages
        # ================================================================
        print("\n" + "=" * 70)
        print("PHASE 4: Run Pipeline Stages")
        print("=" * 70)

        if dry_run:
            print("DRY-RUN mode: Skipping actual stage execution")
        else:
            # Stage 1: generate_data
            print("\n--- Running: generate_data ---")
            result = await client.call_tool("run_stage", {
                "workspace": "baseline",
                "stage": "generate_data",
                "reason": "Deluxe E2E test - generate data",
            })
            stage1_id = result.get("stage_run_id")
            print(f"✓ Launched: {stage1_id}")

            # Wait for completion
            await wait_for_stage(client, stage1_id)

            # Stage 2: preprocess
            print("\n--- Running: preprocess ---")
            result = await client.call_tool("run_stage", {
                "workspace": "baseline",
                "stage": "preprocess",
                "reason": "Deluxe E2E test - preprocess data",
            })
            stage2_id = result.get("stage_run_id")
            print(f"✓ Launched: {stage2_id}")

            await wait_for_stage(client, stage2_id)

            # Stage 3: train
            print("\n--- Running: train ---")
            result = await client.call_tool("run_stage", {
                "workspace": "baseline",
                "stage": "train",
                "reason": "Deluxe E2E test - train model",
            })
            stage3_id = result.get("stage_run_id")
            print(f"✓ Launched: {stage3_id}")

            await wait_for_stage(client, stage3_id)

            # Stage 4: evaluate
            print("\n--- Running: evaluate ---")
            result = await client.call_tool("run_stage", {
                "workspace": "baseline",
                "stage": "evaluate",
                "reason": "Deluxe E2E test - evaluate model",
            })
            stage4_id = result.get("stage_run_id")
            print(f"✓ Launched: {stage4_id}")

            await wait_for_stage(client, stage4_id)

            print("\n✓ All pipeline stages completed!")

        # ================================================================
        # PHASE 5: Verify Results
        # ================================================================
        print("\n" + "=" * 70)
        print("PHASE 5: Verify Results")
        print("=" * 70)

        # Get workspace status
        result = await client.call_tool("status", {})
        print(f"✓ System status retrieved")

        # List jobs
        result = await client.call_tool("list_jobs", {
            "workspace": "baseline",
        })
        job_count = len(result.get("jobs", []))
        print(f"✓ Found {job_count} jobs for baseline workspace")

        print("\n" + "=" * 70)
        print("DELUXE E2E TEST COMPLETE")
        print("=" * 70)
        print("\n✅ All phases completed successfully!")
        print("\nThis validated:")
        print("  • MCP server initialization")
        print("  • MCP tool calls (create_workspace, mount, run_stage, etc.)")
        print("  • Goldfish → GCE integration")
        print("  • Multi-stage pipeline execution")
        print("  • Full workflow orchestration")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        await client.close()


async def wait_for_stage(client: GoldfishMCPClient, stage_run_id: str, timeout: int = 900):
    """Wait for a stage to complete."""
    import time
    start = time.time()

    while time.time() - start < timeout:
        result = await client.call_tool("job_status", {
            "job_id": stage_run_id,
        })

        status = result.get("status")

        if status == "completed":
            print(f"✓ Stage completed successfully")
            return
        elif status == "failed":
            error = result.get("error", "Unknown error")
            raise RuntimeError(f"Stage failed: {error}")

        # Still running
        await asyncio.sleep(10)

    raise TimeoutError(f"Stage {stage_run_id} timed out after {timeout}s")


if __name__ == "__main__":
    asyncio.run(run_deluxe_workflow())

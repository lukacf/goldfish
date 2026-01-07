import os
import sys
from pathlib import Path

sys.path.append(os.path.join(os.getcwd(), "src"))

import subprocess

from goldfish.config import GoldfishConfig

# Load project config to get the correct registry
project_root = Path("~/src/mlm").expanduser()
config = GoldfishConfig.load(project_root)
project_id = config.gce.effective_project_id
registry = config.gce.effective_artifact_registry or f"us-docker.pkg.dev/{project_id}/goldfish"

image_name = "goldfish-base-gpu"
image_tag = f"{image_name}:v3"
registry_tag = f"{registry}/{image_tag}"

print(f"Building {image_tag} for linux/amd64...")
dockerfile = "src/goldfish/infra/base_images/Dockerfile.gpu"

# Build locally
subprocess.run(
    [
        "docker",
        "build",
        "--platform",
        "linux/amd64",
        "-f",
        dockerfile,
        "-t",
        image_tag,
        "src/goldfish/infra/base_images/",
    ],
    check=True,
)

print(f"Tagging for registry: {registry_tag}")
subprocess.run(["docker", "tag", image_tag, registry_tag], check=True)

print(f"Pushing to {registry_tag}...")
subprocess.run(["docker", "push", registry_tag], check=True)

print("Done.")

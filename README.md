# Goldfish

**The Infrastructure Backbone for Autonomous ML Research**

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![MCP Compatible](https://img.shields.io/badge/MCP-compatible-green.svg)](https://modelcontextprotocol.io/)
[![AGPL-3.0 License](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![CI](https://github.com/lukacf/goldfish/actions/workflows/ci.yml/badge.svg)](https://github.com/lukacf/goldfish/actions)

> **Cloud Support**: Production-ready on Google Cloud Platform (GCE + GCS + Artifact Registry).
> The cloud abstraction layer supports adding new backends (AWS, Azure, Kubernetes) by implementing protocols.

---

## The Bridge Between Agentic Code and Production Research

While AI agents are excellent at writing Python, they struggle with the stateful, high-stakes nature of ML infrastructure. Agents lose context after restarts, fail to track exact provenance across iterations, and frequently fall victim to **silent failures**—optimizing models on corrupted data or hallucinating success because they lack mechanistic "eyes" on their outputs.

**Goldfish is the Model Context Protocol (MCP) backbone that transforms an AI agent into a reliable research assistant.** It provides the guardrails, memory, and infrastructure needed to conduct verifiable, reproducible research on real hardware.

---

## Core Pillars of the Goldfish Engine

### 1. Immutable Provenance (Reproducibility by Default)
ML research is only as good as its reproducibility. Goldfish ensures that every result is tied to the exact code and configuration that produced it.
- **Copy-Based Isolation:** Agents work on ephemeral file copies in "slots" while versioning is enforced in a hidden git-backend.
- **Atomic Run-Commits:** Every `run()` call triggers an automatic sync and commit. You can rollback to any previous experiment state with 100% fidelity.
- **Data Lineage:** Full provenance tracking from raw datasets through features and models to final metrics.

### 2. Multi-Phase Integrity Guard (Detecting Silent Failures)
Goldfish ends the "Garbage In, Garbage Out" cycle by enforcing rigorous data contracts and multi-stage verification:
- **Schema-Based Contracts (The Law):** Define strict expectations for signal geometry (shape, dtype) and distribution stats. Mechanistic checks catch "dead" datasets or collapsed distributions (e.g., sine-wave hallucinations) before they corrupt the pipeline.
- **Pre-Run AI Review:** Before execution, an AI agent reviews the entire workspace context—code, config, and git diff—to catch logic errors, missing imports, or hypothesis-code mismatches.
- **Runtime & Post-Run Verification:** Combines real-time health monitoring (loss/grad norm) with post-stage semantic review, where AI evaluates artifacts alongside their statistical profiles to ensure results align with the experimental intent.

### 3. Transparent Compute Fabric (Hiding Infrastructure Hell)
Goldfish abstracts away "GCP compatibility matrix hell" and Docker plumbing so the agent can focus on science.
- **Multi-Backend Execution:** Seamlessly switch between Local Docker for iteration and GCE (H100/A100) for heavy training with a single command.
- **Resource Profiles:** Hardware constraints are managed via high-level profiles (`h100-spot`, `cpu-large`), preventing configuration drift.
- **Managed Storage:** Handles the complex bridging between GCS buckets and high-performance hyperdisks automatically.

### 4. Narrative Context Recovery (Persistent Research Memory)
Experiments that last days shouldn't be lost when an agent's context window refreshes.
- **STATE.md Journaling:** Goldfish maintains a persistent, structured narrative of active goals, configuration invariants, and chronological research progress.
- **Orientation Recovery:** Agents call `status()` to instantly regain situational awareness, seeing active jobs, mounted workspaces, and recent findings.

---

## How It Works

Goldfish provides both a logical framework for research and a physical engine for execution.

### 1. Logical Research Flow (The DAG)
Experiments are organized as a Directed Acyclic Graph (DAG) of **Stages** and **Signals**, where every node is versioned and every edge is typed.

```text
┌─────────────────────────────────────────────────────────────────────┐
│                         WORKSPACE (The Lab)                         │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────────┐   │
│   │                        PIPELINE (DAG)                       │   │
│   │    ┌──────────┐      ┌──────────┐      ┌──────────┐         │   │
│   │    │  STAGE   │      │  STAGE   │      │  STAGE   │         │   │
│   │    │preprocess│─npy─▶│  train   │─dir─▶│ evaluate │         │   │
│   │    └──────────┘      └──────────┘      └──────────┘         │   │
│   │         ▲                                    │               │   │
│   │         │           SIGNALS (Typed Flow)     ▼               │   │
│   │    ┌─────────┐                         ┌─────────┐          │   │
│   │    │ DATASET │                         │ METRICS │          │   │
│   │    └─────────┘                         └─────────┘          │   │
│   └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│   VERSION: v42 (Immutable Snapshot of Code + Config + Lineage)      │
└─────────────────────────────────────────────────────────────────────┘
```

### 2. Physical Orchestration (The Engine)
Goldfish manages the transition from local agentic code to isolated, containerized execution on high-performance cloud hardware.

```text
       USER ML PROJECT (Local)               GOLDFISH INFRA (Docker/GCE)
┌──────────────────────────────────┐      ┌──────────────────────────────────┐
│  WORKSPACE SLOT (w1, w2, ...)    │      │    CONTAINERIZED STAGE RUN       │
│  ┌────────────────────────────┐  │      │  ┌────────────────────────────┐  │
│  │  modules/train.py          │  │      │  │  [ isolated python env ]   │  │
│  │  configs/train.yaml        │──┼──────┼─▶│  goldfish.io.load_input()  │  │
│  │  pipeline.yaml             │  │      │  │                            │  │
│  └────────────────────────────┘  │      │  │     EXECUTE ML LOGIC       │  │
│               │                  │      │  │   (During-Run Health)      │  │
│               ▼                  │      │  │                            │  │
│    SVS PRE-RUN AI REVIEW         │      │  │  goldfish.io.save_output()  │  │
│    (Logic/Hypothesis Check)      │      │  └──────────────┬─────────────┘  │
│               │                  │      │                 │                │
└───────────────┼──────────────────┘      └─────────────────┼────────────────┘
                │                                           │
                ▼                                           ▼
      IMMUTABLE PROVENANCE                       MECHANISTIC SVS CHECK
     (Git-Backend + Version Tag)               (Shape/Dtype/Entropy/Nulls)
                │                                           │
                ▼                                           ▼
      CLOUD STORAGE (GCS) <──────────────────────> COMPUTE BACKEND (GCE)
      [ Datasets & Artifacts ]                  [ H100 / A100 GPU Nodes ]
```

### 3. Workspace & Provenance Engine (The Storage Layer)
Goldfish uses a **Copy-Based Isolation** model. Agents never touch the main project repository directly; instead, they work in ephemeral "slots" while Goldfish handles the versioning in a hidden backend.

```text
┌─────────────────────────────────────────────────────────────────────┐
│                 GOLDFISH DEV REPO (Internal Backend)                │
│      [ Manages all experiment branches, commits, and tags ]         │
│                                                                     │
│   branch: experiment/axial_attn  ────────────────────────┐          │
│   (Source of Truth)                                      │          │
└──────────────────────────────────────────────────────────┼──────────┘
          ▲                                                │
          │ (2) RUN: Atomic Sync + Commit                  │ (1) MOUNT:
          │     (Provenance Guard)                         │     Copy Files
          │                                                │
┌─────────┼────────────────────────────────────────────────▼──────────┐
│         │            USER ML PROJECT (Editing Slots)                │
│         └──────────  workspaces/w1/  <──────  Agent edits files     │
│                      (Plain files, no .git)                         │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
                   (3) VERSION: axial_attn-v42
                   (Immutable git tag pinned to run SHA)
```

---

## Quick Start

### 1. Install Goldfish
```bash
git clone https://github.com/lukacf/goldfish.git
cd goldfish
pip install -e ".[dev]"
```

### 2. Initialize your Research Repo
Run this in the directory where your ML project lives:
```bash
goldfish init
```

### 3. Connect your AI Agent
Example with Claude Code:
```bash
claude mcp add goldfish -- uv run --directory /path/to/goldfish goldfish serve
```

---

## Documentation

| Document | Audience | Purpose |
|----------|----------|---------|
| [README.md](README.md) | Technical Humans | Evaluation, architecture, and value proposition. |
| [SKILL.md](.claude/skills/goldfish-ml/SKILL.md) | AI Agents | Comprehensive tool reference, schemas, and workflows. |
| [CLAUDE.md](CLAUDE.md) | AI Agents | Internal development guide and technical invariants. |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Human Partners | Development environment and PR process. |
| [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) | Users | Installation and first run guide. |
| [docs/archive/CLOUD_ABSTRACTION.md](docs/archive/CLOUD_ABSTRACTION.md) | Developers | Cloud backend architecture and extension guide. |

---

## License
AGPL-3.0 — see [LICENSE](LICENSE) for details.

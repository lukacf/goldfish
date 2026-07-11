# Goldfish Intro Video (Remotion)

This directory contains a **1 minute intro video** composition for Goldfish.

## Goals

- Put a realistic terminal simulation at center stage.
- Demonstrate why Goldfish is needed for agentic ML research.
- Show key benefits versus traditional ad-hoc ML workflows.
- Make variants easy (e.g. Claude Code now, Codex CLI later).

## Quick start

```bash
cd video
npm install
npm run dev
```

Render:

```bash
npm run render
```

## Customization model

Edit `src/config/variants.ts`:

- `variant.narrative`: Story beats and scene durations.
- `variant.events`: Terminal events with frame offsets.
- `variant.flavor`: Prompt style (`claude-code` or `codex-cli`).

This separation keeps the **terminal engine reusable** while letting you produce multiple story versions.

## Composition details

- `GoldfishIntro` composition is 1920x1080, 30fps, 1800 frames (60 seconds).
- Terminal is built by `TerminalSimulation`, which can support other TUI personas by extending `TerminalFlavor` and event styling.


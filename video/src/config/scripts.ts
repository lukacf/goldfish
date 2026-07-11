/**
 * Terminal demo scripts for Goldfish videos.
 *
 * Each script is an array of ScriptEvents that drive the terminal playback.
 * To create a new video variant, define a new script here and reference it
 * in a new composition.
 *
 * Helper functions (span, line, etc.) keep script definitions readable.
 */

import type { ScriptEvent, TerminalLine, TextSpan } from "../terminal/types";

// ─── Helpers ────────────────────────────────────────────────────────

function span(text: string, color?: string, bold?: boolean): TextSpan {
  return { text, color, bold };
}

function line(...spans: TextSpan[]): TerminalLine {
  return { spans };
}

function textLine(text: string, color?: string): TerminalLine {
  return { spans: [{ text, color }] };
}

function emptyLine(): TerminalLine {
  return { spans: [{ text: "" }] };
}

// ─── Colors (matching Claude Code theme) ────────────────────────────

const GREEN = "#a6e3a1";
const BLUE = "#89b4fa";
const YELLOW = "#f9e2af";
const PURPLE = "#bb9af7";
const DIM = "#565f89";
const TEXT = "#c0caf5";
const GOLD = "#f9a825";
const CYAN = "#89dceb";

// ─── Goldfish Intro Script ──────────────────────────────────────────

/**
 * The main Goldfish intro demo script.
 *
 * Shows Claude Code using Goldfish to:
 * 1. Create a workspace
 * 2. Configure a pipeline
 * 3. Run the pipeline on GPU
 * 4. Get results with automatic versioning
 *
 * Total duration: ~1050 frames (35s at 30fps)
 */
export const goldfishIntroScript: ScriptEvent[] = [
  // ── Claude Code header ────────────────────────────────
  {
    type: "header",
    lines: [
      textLine("╭─────────────────────────────────────────────────╮", DIM),
      line(
        span("│  ", DIM),
        span("✻ ", PURPLE),
        span("Claude Code", TEXT),
        span("              ", TEXT),
        span("OPUS 4", DIM),
        span("             │", DIM)
      ),
      line(
        span("│    ", DIM),
        span("/goldfish", GOLD),
        span(" connected", DIM),
        span("                            │", DIM)
      ),
      textLine("╰─────────────────────────────────────────────────╯", DIM),
      emptyLine(),
    ],
    holdAfter: 25,
  },

  // ── 1. Create workspace ───────────────────────────────
  {
    type: "prompt",
    text: "Create a workspace for our BERT sentiment analysis experiment",
    typingSpeed: 2,
    holdAfter: 8,
  },
  { type: "pause", duration: 5 },
  {
    type: "spinner",
    text: "Creating workspace...",
    duration: 35,
  },
  {
    type: "result",
    lines: [
      line(span("✓ ", GREEN), span("Workspace ", TEXT), span("bert_sentiment", CYAN), span(" created", TEXT)),
      textLine("  Goal: Fine-tune BERT for movie review sentiment classification", DIM),
      line(span("  Mounted at: ", DIM), span("~/workspaces/bert_sentiment/", BLUE)),
      emptyLine(),
    ],
    holdAfter: 30,
  },

  // ── 2. Configure pipeline ─────────────────────────────
  {
    type: "prompt",
    text: "I've written the code. Now configure the pipeline.",
    typingSpeed: 2,
    holdAfter: 8,
  },
  { type: "pause", duration: 5 },
  {
    type: "spinner",
    text: "Reading pipeline.yaml...",
    duration: 30,
  },
  {
    type: "result",
    lines: [
      line(span("✓ ", GREEN), span("Pipeline validated", TEXT)),
      emptyLine(),
      line(span("  preprocess", CYAN), span("  dataset → npy", DIM)),
      line(span("    → ", DIM), span("train", CYAN), span("      npy → directory", DIM)),
      line(span("      → ", DIM), span("evaluate", CYAN), span("  directory → csv", DIM)),
      emptyLine(),
    ],
    holdAfter: 35,
  },

  // ── 3. Run pipeline ───────────────────────────────────
  {
    type: "prompt",
    text: "Run the full pipeline on H100",
    typingSpeed: 2,
    holdAfter: 8,
  },
  { type: "pause", duration: 5 },

  // Sync + commit
  {
    type: "spinner",
    text: "Syncing workspace...",
    duration: 30,
  },
  {
    type: "result",
    lines: [
      line(span("✓ ", GREEN), span("Changes committed — ", TEXT), span("full provenance tracked", YELLOW)),
    ],
    holdAfter: 15,
  },

  // Preprocess
  {
    type: "spinner",
    text: "preprocess running...",
    duration: 35,
  },
  {
    type: "result",
    lines: [
      line(span("✓ ", GREEN), span("preprocess", CYAN), span(" complete ", TEXT), span("(32s)", DIM)),
    ],
    holdAfter: 12,
  },

  // Train
  {
    type: "spinner",
    text: "train running on h100-spot ($2.45/hr)...",
    duration: 50,
  },
  {
    type: "result",
    lines: [
      line(span("✓ ", GREEN), span("train", CYAN), span(" complete", TEXT)),
      line(span("  Loss: ", DIM), span("0.0823", TEXT), span("  |  Accuracy: ", DIM), span("94.7%", GREEN)),
    ],
    holdAfter: 20,
  },

  // Evaluate
  {
    type: "spinner",
    text: "evaluate running...",
    duration: 30,
  },
  {
    type: "result",
    lines: [
      line(span("✓ ", GREEN), span("evaluate", CYAN), span(" complete", TEXT)),
      line(
        span("  F1: ", DIM),
        span("0.943", GREEN),
        span("  |  Precision: ", DIM),
        span("0.951", TEXT),
        span("  |  Recall: ", DIM),
        span("0.935", TEXT)
      ),
      emptyLine(),
    ],
    holdAfter: 25,
  },

  // ── 4. Version created ────────────────────────────────
  {
    type: "output",
    lines: [
      textLine("─────────────────────────────────────────────────", DIM),
      line(span("✓ ", GREEN), span("Version ", TEXT), span("bert_sentiment-v3", GOLD), span(" created", TEXT)),
      line(
        span("  3 stages", CYAN),
        span("  |  ", DIM),
        span("4 signals", CYAN),
        span("  |  ", DIM),
        span("full lineage tracked", YELLOW)
      ),
      textLine("─────────────────────────────────────────────────", DIM),
    ],
    holdAfter: 60,
  },
];

// ─── Script helpers for creating variants ───────────────────────────

/**
 * Creates a variant script by replacing specific events.
 * Useful for A/B testing different demo flows.
 */
export function createVariantScript(
  base: ScriptEvent[],
  replacements: Map<number, ScriptEvent>
): ScriptEvent[] {
  return base.map((event, i) => replacements.get(i) ?? event);
}

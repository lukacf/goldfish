import type { TerminalTheme } from "./types";

const SPINNER_BRAILLE = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/**
 * Claude Code terminal theme.
 * Matches the actual Claude Code CLI appearance.
 */
export const claudeCodeTheme: TerminalTheme = {
  id: "claude-code",
  name: "Claude Code",
  windowTitle: "claude — ~/workspaces",
  bg: "#1a1b26",
  headerBg: "#13141c",
  text: "#c0caf5",
  textDim: "#565f89",
  promptString: "> ",
  promptColor: "#bb9af7",
  cursorColor: "#c0caf5",
  fontFamily: "'JetBrains Mono', 'Fira Code', 'Cascadia Code', monospace",
  fontSize: 16,
  lineHeight: 1.6,
  showTrafficLights: true,
  spinnerChars: SPINNER_BRAILLE,
  spinnerColor: "#7aa2f7",
};

/**
 * Codex CLI terminal theme.
 * Can be customized to match OpenAI Codex CLI appearance.
 */
export const codexCliTheme: TerminalTheme = {
  id: "codex-cli",
  name: "Codex CLI",
  windowTitle: "codex — ~/project",
  bg: "#1e1e1e",
  headerBg: "#181818",
  text: "#d4d4d4",
  textDim: "#6a6a6a",
  promptString: "codex> ",
  promptColor: "#4ec9b0",
  cursorColor: "#d4d4d4",
  fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
  fontSize: 16,
  lineHeight: 1.6,
  showTrafficLights: true,
  spinnerChars: SPINNER_BRAILLE,
  spinnerColor: "#4ec9b0",
};

/**
 * Generic dark terminal theme.
 */
export const genericTheme: TerminalTheme = {
  id: "generic",
  name: "Terminal",
  windowTitle: "Terminal",
  bg: "#1c1c1c",
  headerBg: "#2d2d2d",
  text: "#e0e0e0",
  textDim: "#808080",
  promptString: "$ ",
  promptColor: "#98c379",
  cursorColor: "#e0e0e0",
  fontFamily: "'JetBrains Mono', monospace",
  fontSize: 16,
  lineHeight: 1.6,
  showTrafficLights: true,
  spinnerChars: ["|", "/", "-", "\\"],
  spinnerColor: "#98c379",
};

/** Theme registry — add new themes here */
export const themes: Record<string, TerminalTheme> = {
  "claude-code": claudeCodeTheme,
  "codex-cli": codexCliTheme,
  generic: genericTheme,
};

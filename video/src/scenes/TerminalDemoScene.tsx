import React from "react";
import { useCurrentFrame, interpolate } from "remotion";
import { Terminal } from "../terminal";
import type { ScriptEvent, TerminalTheme } from "../terminal/types";
import { COLORS } from "../config/video";

interface TerminalDemoSceneProps {
  /** The terminal script to play */
  script: ScriptEvent[];
  /** The terminal theme */
  theme: TerminalTheme;
  /** Optional caption shown above the terminal */
  caption?: string;
}

/**
 * The core terminal demo scene (36 seconds).
 *
 * This is the centerpiece of the video — a realistic terminal simulation
 * showing Claude Code using Goldfish MCP tools.
 *
 * Props are fully customizable so this scene can be reused for different
 * TUI tools (Codex CLI, etc.) by swapping script + theme.
 */
export const TerminalDemoScene: React.FC<TerminalDemoSceneProps> = ({
  script,
  theme,
  caption,
}) => {
  const frame = useCurrentFrame();

  // Terminal slides up into view
  const terminalY = interpolate(frame, [0, 20], [40, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  const terminalOpacity = interpolate(frame, [0, 15], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  // Caption fades in above terminal
  const captionOpacity = caption
    ? interpolate(frame, [5, 20], [0, 1], {
        extrapolateLeft: "clamp",
        extrapolateRight: "clamp",
      })
    : 0;

  return (
    <div
      style={{
        width: "100%",
        height: "100%",
        display: "flex",
        flexDirection: "column",
        justifyContent: "center",
        alignItems: "center",
        background: COLORS.bg,
        padding: "30px 60px",
      }}
    >
      {/* Optional caption */}
      {caption && (
        <div
          style={{
            fontSize: 22,
            fontFamily: "'Inter', 'Helvetica Neue', sans-serif",
            color: COLORS.textDim,
            marginBottom: 20,
            opacity: captionOpacity,
            letterSpacing: "0.02em",
          }}
        >
          {caption}
        </div>
      )}

      {/* Terminal */}
      <div
        style={{
          opacity: terminalOpacity,
          transform: `translateY(${terminalY}px)`,
          width: "100%",
          display: "flex",
          justifyContent: "center",
        }}
      >
        <Terminal
          script={script}
          theme={theme}
          width="100%"
          maxHeight="90%"
        />
      </div>
    </div>
  );
};

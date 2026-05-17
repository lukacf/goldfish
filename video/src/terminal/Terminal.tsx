import React from "react";
import { useCurrentFrame } from "remotion";
import type { ScriptEvent, TerminalTheme } from "./types";
import { useTerminalPlayback } from "./useTerminalPlayback";
import { TerminalChrome } from "./TerminalChrome";
import { TerminalContent } from "./TerminalContent";

interface TerminalProps {
  /** The script to play back */
  script: ScriptEvent[];
  /** Visual theme */
  theme: TerminalTheme;
  /** Override the frame (defaults to Remotion's useCurrentFrame) */
  frame?: number;
  /** Terminal width as CSS value. Default: "90%" */
  width?: string;
  /** Terminal max-height as CSS value. Default: "85%" */
  maxHeight?: string;
}

/**
 * The main Terminal component.
 *
 * Renders a realistic terminal window with chrome (title bar + traffic lights)
 * and animated content driven by a script + theme.
 *
 * Usage:
 *   <Terminal script={myScript} theme={claudeCodeTheme} />
 *
 * To simulate a different TUI tool, swap the theme and script.
 */
export const Terminal: React.FC<TerminalProps> = ({
  script,
  theme,
  frame: frameProp,
  width = "90%",
  maxHeight = "85%",
}) => {
  const remotionFrame = useCurrentFrame();
  const frame = frameProp ?? remotionFrame;
  const state = useTerminalPlayback(script, frame, theme);

  return (
    <div
      style={{
        width,
        maxHeight,
        display: "flex",
        flexDirection: "column",
        borderRadius: 12,
        overflow: "hidden",
        boxShadow: "0 25px 60px rgba(0, 0, 0, 0.6), 0 0 120px rgba(0, 0, 0, 0.3)",
        border: "1px solid rgba(255, 255, 255, 0.06)",
      }}
    >
      <TerminalChrome theme={theme} />
      <TerminalContent state={state} theme={theme} />
    </div>
  );
};

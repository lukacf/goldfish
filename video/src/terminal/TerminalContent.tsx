import React from "react";
import type { TerminalLine, TerminalState, TerminalTheme, TextSpan } from "./types";

interface TerminalContentProps {
  state: TerminalState;
  theme: TerminalTheme;
}

/**
 * Renders the terminal body: scrollback lines, current typing, spinner, and cursor.
 */
export const TerminalContent: React.FC<TerminalContentProps> = ({
  state,
  theme,
}) => {
  return (
    <div
      style={{
        background: theme.bg,
        padding: "16px 20px",
        fontFamily: theme.fontFamily,
        fontSize: theme.fontSize,
        lineHeight: theme.lineHeight,
        color: theme.text,
        flex: 1,
        overflow: "hidden",
        minHeight: 400,
      }}
    >
      {/* Completed lines */}
      {state.lines.map((line, i) => (
        <LineRenderer key={i} line={line} theme={theme} />
      ))}

      {/* Current typing line */}
      {state.currentTyping && (
        <div style={{ whiteSpace: "pre" }}>
          <span style={{ color: theme.promptColor, fontWeight: "bold" }}>
            {state.currentTyping.prompt}
          </span>
          <span style={{ color: theme.text }}>
            {state.currentTyping.text.slice(0, state.currentTyping.visibleChars)}
          </span>
          {state.showCursor && <Cursor color={theme.cursorColor} />}
        </div>
      )}

      {/* Active spinner */}
      {state.activeSpinner && (
        <div style={{ whiteSpace: "pre" }}>
          <span style={{ color: theme.spinnerColor }}>
            {theme.spinnerChars[state.activeSpinner.charIndex]}
          </span>
          <span style={{ color: theme.textDim }}>
            {" "}
            {state.activeSpinner.text}
          </span>
        </div>
      )}

      {/* Idle cursor (no typing, no spinner) */}
      {!state.currentTyping && !state.activeSpinner && state.showCursor && (
        <div style={{ whiteSpace: "pre" }}>
          <span style={{ color: theme.promptColor, fontWeight: "bold" }}>
            {theme.promptString}
          </span>
          <Cursor color={theme.cursorColor} />
        </div>
      )}
    </div>
  );
};

/** Renders a single terminal line from its spans */
const LineRenderer: React.FC<{ line: TerminalLine; theme: TerminalTheme }> = ({
  line,
  theme,
}) => {
  return (
    <div style={{ whiteSpace: "pre-wrap", wordBreak: "break-word" }}>
      {line.spans.map((span, i) => (
        <SpanRenderer key={i} span={span} theme={theme} />
      ))}
    </div>
  );
};

/** Renders a single styled text span */
const SpanRenderer: React.FC<{ span: TextSpan; theme: TerminalTheme }> = ({
  span,
  theme,
}) => {
  return (
    <span
      style={{
        color: span.color ?? theme.text,
        fontWeight: span.bold ? "bold" : "normal",
        opacity: span.dim ? 0.5 : 1,
      }}
    >
      {span.text}
    </span>
  );
};

/** Block cursor */
const Cursor: React.FC<{ color: string }> = ({ color }) => {
  return (
    <span
      style={{
        display: "inline-block",
        width: "0.6em",
        height: "1.1em",
        background: color,
        verticalAlign: "text-bottom",
        marginLeft: 1,
        opacity: 0.8,
      }}
    />
  );
};

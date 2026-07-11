import React from "react";
import type { TerminalTheme } from "./types";

interface TerminalChromeProps {
  theme: TerminalTheme;
}

/**
 * macOS-style window chrome for the terminal.
 * Renders traffic lights and a centered title.
 */
export const TerminalChrome: React.FC<TerminalChromeProps> = ({ theme }) => {
  return (
    <div
      style={{
        background: theme.headerBg,
        padding: "10px 16px",
        display: "flex",
        alignItems: "center",
        position: "relative",
        minHeight: 20,
        borderBottom: "1px solid rgba(255, 255, 255, 0.04)",
      }}
    >
      {theme.showTrafficLights && (
        <div style={{ display: "flex", gap: 8 }}>
          <div style={trafficLight("#ff5f57")} />
          <div style={trafficLight("#febc2e")} />
          <div style={trafficLight("#28c840")} />
        </div>
      )}
      <div
        style={{
          position: "absolute",
          left: "50%",
          transform: "translateX(-50%)",
          color: theme.textDim,
          fontSize: 13,
          fontFamily: theme.fontFamily,
          userSelect: "none",
        }}
      >
        {theme.windowTitle}
      </div>
    </div>
  );
};

function trafficLight(color: string): React.CSSProperties {
  return {
    width: 12,
    height: 12,
    borderRadius: "50%",
    background: color,
  };
}

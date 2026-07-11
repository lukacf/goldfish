import React from "react";
import { useCurrentFrame, interpolate, spring, useVideoConfig } from "remotion";
import { COLORS } from "../config/video";

/**
 * Closing scene (3 seconds).
 *
 * Shows the Goldfish name + a call-to-action.
 */
export const ClosingScene: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  const scale = spring({
    frame,
    fps,
    config: { damping: 12, stiffness: 80, mass: 0.8 },
  });

  const subtitleOpacity = interpolate(frame, [15, 30], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

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
      }}
    >
      <div
        style={{
          transform: `scale(${scale})`,
          fontSize: 72,
          fontWeight: 700,
          fontFamily: "'Inter', 'Helvetica Neue', sans-serif",
          color: COLORS.gold,
          textShadow: `0 0 60px ${COLORS.gold}40`,
          letterSpacing: "-0.02em",
        }}
      >
        Goldfish
      </div>

      <div
        style={{
          marginTop: 20,
          fontSize: 22,
          fontFamily: "'JetBrains Mono', monospace",
          color: COLORS.textDim,
          opacity: subtitleOpacity,
          letterSpacing: "0.02em",
        }}
      >
        Let your AI agent run the experiment.
      </div>
    </div>
  );
};

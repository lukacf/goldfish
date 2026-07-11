import React from "react";
import { useCurrentFrame, interpolate, spring, useVideoConfig } from "remotion";
import { COLORS } from "../config/video";

/**
 * Opening title card (5 seconds).
 *
 * Shows:
 * - "Goldfish" name with gold accent
 * - Tagline: "Agentic ML Experimentation"
 * - Subtle animated underline
 */
export const TitleScene: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // Logo scale spring animation
  const logoScale = spring({
    frame,
    fps,
    config: { damping: 12, stiffness: 80, mass: 0.8 },
  });

  // Tagline fades in after logo
  const taglineOpacity = interpolate(frame, [30, 50], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  const taglineY = interpolate(frame, [30, 50], [15, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  // Underline draws in
  const underlineWidth = interpolate(frame, [50, 80], [0, 300], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  // Fade out at end
  const fadeOut = interpolate(frame, [120, 150], [1, 0], {
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
        opacity: fadeOut,
      }}
    >
      {/* Logo / Name */}
      <div
        style={{
          transform: `scale(${logoScale})`,
          fontSize: 84,
          fontWeight: 700,
          fontFamily: "'Inter', 'Helvetica Neue', sans-serif",
          letterSpacing: "-0.02em",
          color: COLORS.gold,
          textShadow: `0 0 60px ${COLORS.gold}40, 0 0 120px ${COLORS.gold}20`,
        }}
      >
        Goldfish
      </div>

      {/* Underline accent */}
      <div
        style={{
          width: underlineWidth,
          height: 3,
          background: `linear-gradient(90deg, transparent, ${COLORS.gold}, transparent)`,
          marginTop: 8,
          borderRadius: 2,
        }}
      />

      {/* Tagline */}
      <div
        style={{
          marginTop: 24,
          fontSize: 28,
          fontWeight: 400,
          fontFamily: "'Inter', 'Helvetica Neue', sans-serif",
          color: COLORS.textDim,
          opacity: taglineOpacity,
          transform: `translateY(${taglineY}px)`,
          letterSpacing: "0.04em",
        }}
      >
        Agentic ML Experimentation
      </div>
    </div>
  );
};

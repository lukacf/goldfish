import React from "react";
import { useCurrentFrame, interpolate } from "remotion";
import { COLORS } from "../config/video";

interface PainPoint {
  icon: string;
  text: string;
  color: string;
}

const PAIN_POINTS: PainPoint[] = [
  { icon: "✗", text: "Experiments are impossible to reproduce", color: COLORS.red },
  { icon: "✗", text: "Manual tracking — work gets lost", color: COLORS.red },
  { icon: "✗", text: "GPU infrastructure is complex to manage", color: COLORS.red },
  { icon: "✗", text: "No provenance — no trust in results", color: COLORS.red },
];

/**
 * Problem statement scene (9 seconds).
 *
 * Reveals pain points one by one with a "traditional ML workflow" framing,
 * building the case for why Goldfish exists.
 */
export const ProblemScene: React.FC = () => {
  const frame = useCurrentFrame();

  // Title fade in
  const titleOpacity = interpolate(frame, [0, 15], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  // Scene fade out
  const fadeOut = interpolate(frame, [240, 270], [1, 0], {
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
      {/* Section title */}
      <div
        style={{
          fontSize: 38,
          fontWeight: 600,
          fontFamily: "'Inter', 'Helvetica Neue', sans-serif",
          color: COLORS.text,
          opacity: titleOpacity,
          marginBottom: 50,
        }}
      >
        Traditional ML workflow
      </div>

      {/* Pain points */}
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: 28,
          alignItems: "flex-start",
        }}
      >
        {PAIN_POINTS.map((point, i) => {
          // Stagger: each point appears 40 frames after the previous
          const startFrame = 30 + i * 40;
          const opacity = interpolate(frame, [startFrame, startFrame + 15], [0, 1], {
            extrapolateLeft: "clamp",
            extrapolateRight: "clamp",
          });
          const translateX = interpolate(frame, [startFrame, startFrame + 15], [-30, 0], {
            extrapolateLeft: "clamp",
            extrapolateRight: "clamp",
          });

          return (
            <div
              key={i}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 16,
                opacity,
                transform: `translateX(${translateX}px)`,
              }}
            >
              <span
                style={{
                  fontSize: 26,
                  color: point.color,
                  fontWeight: "bold",
                  fontFamily: "'JetBrains Mono', monospace",
                }}
              >
                {point.icon}
              </span>
              <span
                style={{
                  fontSize: 26,
                  color: COLORS.text,
                  fontFamily: "'Inter', 'Helvetica Neue', sans-serif",
                  fontWeight: 400,
                }}
              >
                {point.text}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
};

import React from "react";
import { useCurrentFrame, interpolate, spring, useVideoConfig } from "remotion";
import { COLORS } from "../config/video";

interface Benefit {
  icon: string;
  title: string;
  subtitle: string;
  color: string;
}

const BENEFITS: Benefit[] = [
  {
    icon: "✓",
    title: "100% Reproducible",
    subtitle: "Every run is versioned — nothing is ever lost",
    color: COLORS.green,
  },
  {
    icon: "✓",
    title: "Automatic Provenance",
    subtitle: "Full lineage from data to results",
    color: COLORS.green,
  },
  {
    icon: "✓",
    title: "Cloud-Native Execution",
    subtitle: "H100s on demand, spot pricing, zero config",
    color: COLORS.green,
  },
  {
    icon: "✓",
    title: "AI-Native Workflow",
    subtitle: "Built for agents — not adapted for them",
    color: COLORS.green,
  },
];

/**
 * Benefits overview scene (7 seconds).
 *
 * Shows four key benefits in a 2x2 grid with staggered spring animations.
 */
export const BenefitsScene: React.FC = () => {
  const frame = useCurrentFrame();
  const { fps } = useVideoConfig();

  // Title
  const titleOpacity = interpolate(frame, [0, 12], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  // Fade out
  const fadeOut = interpolate(frame, [180, 210], [1, 0], {
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
        padding: "40px 80px",
      }}
    >
      {/* Heading */}
      <div
        style={{
          fontSize: 38,
          fontWeight: 600,
          fontFamily: "'Inter', 'Helvetica Neue', sans-serif",
          color: COLORS.gold,
          opacity: titleOpacity,
          marginBottom: 50,
        }}
      >
        Why Goldfish?
      </div>

      {/* Benefits grid */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: "36px 80px",
          maxWidth: 1100,
        }}
      >
        {BENEFITS.map((benefit, i) => {
          const delay = 10 + i * 12;
          const scale = spring({
            frame: frame - delay,
            fps,
            config: { damping: 14, stiffness: 100, mass: 0.6 },
          });
          const opacity = interpolate(frame, [delay, delay + 12], [0, 1], {
            extrapolateLeft: "clamp",
            extrapolateRight: "clamp",
          });

          return (
            <div
              key={i}
              style={{
                display: "flex",
                gap: 16,
                alignItems: "flex-start",
                opacity,
                transform: `scale(${scale})`,
              }}
            >
              <span
                style={{
                  fontSize: 30,
                  color: benefit.color,
                  fontWeight: "bold",
                  fontFamily: "'JetBrains Mono', monospace",
                  lineHeight: 1.2,
                  flexShrink: 0,
                }}
              >
                {benefit.icon}
              </span>
              <div>
                <div
                  style={{
                    fontSize: 24,
                    fontWeight: 600,
                    fontFamily: "'Inter', 'Helvetica Neue', sans-serif",
                    color: COLORS.text,
                    marginBottom: 4,
                  }}
                >
                  {benefit.title}
                </div>
                <div
                  style={{
                    fontSize: 18,
                    fontFamily: "'Inter', 'Helvetica Neue', sans-serif",
                    color: COLORS.textDim,
                  }}
                >
                  {benefit.subtitle}
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

import React from "react";
import { useCurrentFrame, useVideoConfig, interpolate } from "remotion";

interface SceneFadeProps {
  children: React.ReactNode;
  /** Frames for the fade-in at the start. Default: 10 */
  fadeInDuration?: number;
  /** Frames for the fade-out at the end. Default: 10 */
  fadeOutDuration?: number;
}

/**
 * Wraps a scene with fade-in at the start and fade-out at the end.
 * Uses the parent Sequence's duration for timing.
 */
export const SceneFade: React.FC<SceneFadeProps> = ({
  children,
  fadeInDuration = 10,
  fadeOutDuration = 10,
}) => {
  const frame = useCurrentFrame();
  const { durationInFrames } = useVideoConfig();

  const opacity = interpolate(
    frame,
    [0, fadeInDuration, durationInFrames - fadeOutDuration, durationInFrames],
    [0, 1, 1, 0],
    { extrapolateLeft: "clamp", extrapolateRight: "clamp" }
  );

  return <div style={{ opacity, width: "100%", height: "100%" }}>{children}</div>;
};

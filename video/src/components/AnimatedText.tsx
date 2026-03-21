import React from "react";
import { useCurrentFrame, interpolate } from "remotion";

interface AnimatedTextProps {
  children: string;
  /** Frame at which the animation starts (relative to parent Sequence) */
  startFrame?: number;
  /** Duration of the fade-in (frames). Default: 15 */
  fadeIn?: number;
  /** Optional vertical slide distance in px. Default: 20 */
  slideUp?: number;
  style?: React.CSSProperties;
}

/**
 * Text component with fade-in + slide-up animation.
 * Useful for title cards, bullet points, captions.
 */
export const AnimatedText: React.FC<AnimatedTextProps> = ({
  children,
  startFrame = 0,
  fadeIn = 15,
  slideUp = 20,
  style,
}) => {
  const frame = useCurrentFrame();
  const progress = frame - startFrame;

  const opacity = interpolate(progress, [0, fadeIn], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  const translateY = interpolate(progress, [0, fadeIn], [slideUp, 0], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
  });

  return (
    <div
      style={{
        opacity,
        transform: `translateY(${translateY}px)`,
        ...style,
      }}
    >
      {children}
    </div>
  );
};

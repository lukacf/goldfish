/**
 * Video configuration constants.
 * Change these to adjust the global video properties.
 */
export const VIDEO_CONFIG = {
  fps: 30,
  width: 1920,
  height: 1080,
  durationInSeconds: 60,
} as const;

export const TOTAL_FRAMES = VIDEO_CONFIG.fps * VIDEO_CONFIG.durationInSeconds;

/**
 * Scene durations in seconds. Must sum to VIDEO_CONFIG.durationInSeconds.
 * Adjust these to re-balance the video pacing.
 */
export const SCENE_DURATIONS = {
  title: 5,
  problem: 9,
  terminalDemo: 36,
  benefits: 7,
  closing: 3,
} as const;

/** Scene durations converted to frames for Remotion <Sequence> */
export const SCENE_FRAMES = {
  title: SCENE_DURATIONS.title * VIDEO_CONFIG.fps,
  problem: SCENE_DURATIONS.problem * VIDEO_CONFIG.fps,
  terminalDemo: SCENE_DURATIONS.terminalDemo * VIDEO_CONFIG.fps,
  benefits: SCENE_DURATIONS.benefits * VIDEO_CONFIG.fps,
  closing: SCENE_DURATIONS.closing * VIDEO_CONFIG.fps,
} as const;

/** Colors used throughout the video */
export const COLORS = {
  bg: "#0a0a1a",
  terminalBg: "#1e1e2e",
  terminalHeaderBg: "#181825",
  text: "#cdd6f4",
  textDim: "#6c7086",
  green: "#a6e3a1",
  blue: "#89b4fa",
  yellow: "#f9e2af",
  red: "#f38ba8",
  purple: "#cba6f7",
  gold: "#f9a825",
  goldLight: "#fdd835",
  surface: "#11111b",
  overlay: "#313244",
} as const;

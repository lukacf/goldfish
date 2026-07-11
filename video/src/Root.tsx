import React from "react";
import { Composition } from "remotion";
import { GoldfishIntro } from "./GoldfishIntro";
import { VIDEO_CONFIG, TOTAL_FRAMES } from "./config/video";

/**
 * Root component that registers all video compositions.
 *
 * To add a new video variant:
 *   1. Create a new composition component (e.g., GoldfishIntroV2.tsx)
 *   2. Add a new <Composition> entry here
 *   3. Render with: npx remotion render <CompositionId>
 */
export const RemotionRoot: React.FC = () => {
  return (
    <>
      {/* Main intro video — 1 minute, 1080p, 30fps */}
      <Composition
        id="GoldfishIntro"
        component={GoldfishIntro}
        durationInFrames={TOTAL_FRAMES}
        fps={VIDEO_CONFIG.fps}
        width={VIDEO_CONFIG.width}
        height={VIDEO_CONFIG.height}
      />

      {/* Add more compositions here for variants, e.g.:
      <Composition
        id="GoldfishIntroShort"
        component={GoldfishIntroShort}
        durationInFrames={900}
        fps={30}
        width={1920}
        height={1080}
      />
      */}
    </>
  );
};

import React from "react";
import { Sequence } from "remotion";
import { SCENE_FRAMES } from "./config/video";
import { goldfishIntroScript } from "./config/scripts";
import { claudeCodeTheme } from "./terminal";
import {
  TitleScene,
  ProblemScene,
  TerminalDemoScene,
  BenefitsScene,
  ClosingScene,
} from "./scenes";

/**
 * GoldfishIntro — the main 60-second intro video composition.
 *
 * Structure:
 *   [0-5s]   Title card
 *   [5-14s]  Problem statement (why Goldfish?)
 *   [14-50s] Terminal demo (Claude Code + Goldfish)
 *   [50-57s] Benefits summary
 *   [57-60s] Closing CTA
 *
 * To create a variant:
 *   1. Duplicate this file
 *   2. Swap the script/theme in TerminalDemoScene
 *   3. Register the new composition in Root.tsx
 */
export const GoldfishIntro: React.FC = () => {
  let offset = 0;

  const titleStart = offset;
  offset += SCENE_FRAMES.title;

  const problemStart = offset;
  offset += SCENE_FRAMES.problem;

  const terminalStart = offset;
  offset += SCENE_FRAMES.terminalDemo;

  const benefitsStart = offset;
  offset += SCENE_FRAMES.benefits;

  const closingStart = offset;

  return (
    <div style={{ background: "#0a0a1a", width: "100%", height: "100%" }}>
      {/* 1. Title */}
      <Sequence from={titleStart} durationInFrames={SCENE_FRAMES.title} name="Title">
        <TitleScene />
      </Sequence>

      {/* 2. Problem */}
      <Sequence from={problemStart} durationInFrames={SCENE_FRAMES.problem} name="Problem">
        <ProblemScene />
      </Sequence>

      {/* 3. Terminal Demo */}
      <Sequence
        from={terminalStart}
        durationInFrames={SCENE_FRAMES.terminalDemo}
        name="Terminal Demo"
      >
        <TerminalDemoScene
          script={goldfishIntroScript}
          theme={claudeCodeTheme}
        />
      </Sequence>

      {/* 4. Benefits */}
      <Sequence from={benefitsStart} durationInFrames={SCENE_FRAMES.benefits} name="Benefits">
        <BenefitsScene />
      </Sequence>

      {/* 5. Closing */}
      <Sequence from={closingStart} durationInFrames={SCENE_FRAMES.closing} name="Closing">
        <ClosingScene />
      </Sequence>
    </div>
  );
};

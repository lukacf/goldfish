# Goldfish Video

Remotion-based intro videos for Goldfish. Built for creating multiple versions with a reusable, customizable terminal simulation component.

## Quick Start

```bash
cd video
npm install

# Open Remotion Studio (interactive preview)
npm run studio

# Render to MP4
npm run render
# Output: out/goldfish-intro.mp4
```

## Project Structure

```
video/
├── src/
│   ├── index.ts                    # Remotion entry point
│   ├── Root.tsx                    # Composition registry
│   ├── GoldfishIntro.tsx           # Main 60s intro video
│   ├── config/
│   │   ├── video.ts                # FPS, dimensions, scene durations
│   │   └── scripts.ts              # Terminal demo scripts
│   ├── terminal/                   # ⭐ Reusable terminal simulator
│   │   ├── types.ts                # Core types (ScriptEvent, Theme, etc.)
│   │   ├── themes.ts               # Themes: claude-code, codex-cli, generic
│   │   ├── Terminal.tsx             # Main terminal component
│   │   ├── TerminalChrome.tsx       # Window chrome (traffic lights)
│   │   ├── TerminalContent.tsx      # Content renderer
│   │   └── useTerminalPlayback.ts   # Script playback engine
│   ├── scenes/                     # Video scenes (composable)
│   │   ├── TitleScene.tsx
│   │   ├── ProblemScene.tsx
│   │   ├── TerminalDemoScene.tsx
│   │   ├── BenefitsScene.tsx
│   │   └── ClosingScene.tsx
│   └── components/                 # Shared animation primitives
│       ├── AnimatedText.tsx
│       └── SceneFade.tsx
└── remotion.config.ts
```

## Creating a New Video Version

### 1. Change the terminal content

Edit `src/config/scripts.ts` to modify what the terminal demo shows. The script is an array of events:

```ts
const myScript: ScriptEvent[] = [
  { type: "header", lines: [...], holdAfter: 20 },
  { type: "prompt", text: "User types this", typingSpeed: 2 },
  { type: "spinner", text: "Loading...", duration: 40 },
  { type: "result", lines: [...], holdAfter: 15 },
  { type: "output", lines: [...] },
  { type: "pause", duration: 30 },
];
```

### 2. Simulate a different TUI tool

Swap the theme to change the terminal's visual identity:

```tsx
import { codexCliTheme } from "./terminal";

<TerminalDemoScene
  script={codexScript}
  theme={codexCliTheme}
/>
```

Add a new theme in `src/terminal/themes.ts`:

```ts
export const myToolTheme: TerminalTheme = {
  id: "my-tool",
  name: "My Tool",
  windowTitle: "my-tool — ~/project",
  bg: "#1e1e1e",
  // ... see themes.ts for all fields
};
```

### 3. Create a new composition variant

1. Copy `GoldfishIntro.tsx` → `GoldfishIntroV2.tsx`
2. Modify scenes, timing, or content
3. Register in `Root.tsx`:

```tsx
<Composition
  id="GoldfishIntroV2"
  component={GoldfishIntroV2}
  durationInFrames={1800}
  fps={30}
  width={1920}
  height={1080}
/>
```

4. Render: `npx remotion render GoldfishIntroV2 out/v2.mp4`

### 4. Adjust scene timing

Edit `src/config/video.ts`:

```ts
export const SCENE_DURATIONS = {
  title: 5,        // seconds
  problem: 9,
  terminalDemo: 36,
  benefits: 7,
  closing: 3,
};
```

## Terminal Simulation Architecture

The terminal simulator is designed to be **tool-agnostic**:

- **Themes** control visual appearance (colors, prompt string, spinner style, font)
- **Scripts** control content (what gets typed, what output appears, timing)
- **Playback hook** (`useTerminalPlayback`) computes terminal state at any frame

This means you can simulate any TUI-based tool (Claude Code, Codex CLI, Cursor, Aider, etc.) by providing the right theme + script combination without changing any component code.

### Script Event Types

| Event     | Description                                    |
|-----------|------------------------------------------------|
| `header`  | Box-drawn header (appears instantly)           |
| `prompt`  | User typing animation with configurable speed  |
| `spinner` | Braille/character spinner with loading text    |
| `result`  | Output lines replacing a spinner               |
| `output`  | Instant text output                            |
| `pause`   | Empty time gap                                 |
| `clear`   | Clears the terminal                            |

## Rendering

```bash
# MP4 (default)
npx remotion render GoldfishIntro out/intro.mp4

# Custom quality
npx remotion render GoldfishIntro out/intro.mp4 --crf 18

# GIF
npx remotion render GoldfishIntro out/intro.gif

# Specific frame range (for testing)
npx remotion render GoldfishIntro out/clip.mp4 --frames=420-1500
```

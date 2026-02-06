export type TerminalEventType = 'prompt' | 'command' | 'output' | 'system';

export type TerminalEvent = {
  atFrame: number;
  type: TerminalEventType;
  content: string;
};

export type SceneBeat = {
  title: string;
  subtitle: string;
  durationFrames: number;
};

export type NarrativeInput = {
  terminalTitle: string;
  problem: string;
  solution: string;
  cta: string;
  scenes?: SceneBeat[];
};

export type Narrative = {
  terminalTitle: string;
  scenes: SceneBeat[];
};

const DEFAULT_SCENES: SceneBeat[] = [
  {
    title: 'Problem: agentic ML work is hard to trust',
    subtitle: 'Traditional scripts and ad-hoc notebooks hide provenance and break reproducibility.',
    durationFrames: 300,
  },
  {
    title: 'Goldfish gives Claude Code a safe ML operating layer',
    subtitle: 'Workspaces, versions, and pipeline execution become tools instead of brittle glue code.',
    durationFrames: 360,
  },
  {
    title: 'Live run: create workspace → run pipeline → inspect lineage',
    subtitle: 'The terminal simulation mirrors real Claude Code + Goldfish interactions.',
    durationFrames: 540,
  },
  {
    title: 'Why it wins vs traditional approaches',
    subtitle: 'Copy-based isolation, immutable versions, typed signals, and backend abstraction.',
    durationFrames: 420,
  },
  {
    title: 'CTA',
    subtitle: 'Adopt Goldfish to accelerate reproducible agentic ML research.',
    durationFrames: 180,
  },
];

export const buildTerminalFrames = (
  events: TerminalEvent[],
  frame: number,
): TerminalEvent[] => {
  return [...events]
    .sort((a, b) => a.atFrame - b.atFrame)
    .filter((event) => event.atFrame <= frame);
};

export const buildVariantNarrative = (input: NarrativeInput): Narrative => {
  if (input.scenes) {
    return {terminalTitle: input.terminalTitle, scenes: input.scenes};
  }

  const scenes = [...DEFAULT_SCENES];
  scenes[0] = {...scenes[0], subtitle: input.problem};
  scenes[1] = {...scenes[1], subtitle: input.solution};
  scenes[4] = {...scenes[4], subtitle: input.cta};

  return {
    terminalTitle: input.terminalTitle,
    scenes,
  };
};

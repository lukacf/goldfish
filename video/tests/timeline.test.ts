import {describe, expect, test} from 'vitest';

import {buildTerminalFrames, buildVariantNarrative, type TerminalEvent} from '../src/lib/timeline';

const sampleEvents: TerminalEvent[] = [
  {atFrame: 0, type: 'prompt', content: 'claude code --mcp goldfish'},
  {atFrame: 20, type: 'output', content: 'Connected to Goldfish.'},
  {atFrame: 60, type: 'command', content: 'goldfish create_workspace baseline_lstm'},
  {atFrame: 90, type: 'output', content: 'Workspace created.'},
];

describe('buildTerminalFrames', () => {
  test('reveals events only after their frame offset', () => {
    expect(buildTerminalFrames(sampleEvents, 10)).toHaveLength(1);
    expect(buildTerminalFrames(sampleEvents, 89)).toHaveLength(3);
    expect(buildTerminalFrames(sampleEvents, 100)).toHaveLength(4);
  });

  test('returns stable ordering by frame even if unsorted input', () => {
    const unsorted = [...sampleEvents].reverse();
    const visible = buildTerminalFrames(unsorted, 100);
    expect(visible.map((event) => event.content)).toEqual(sampleEvents.map((event) => event.content));
  });
});

describe('buildVariantNarrative', () => {
  test('fills defaults when optional scenes are not provided', () => {
    const narrative = buildVariantNarrative({
      terminalTitle: 'Claude Code',
      problem: 'Traditional ML ops are brittle',
      solution: 'Goldfish makes runs reproducible',
      cta: 'Start now',
    });

    expect(narrative.scenes).toHaveLength(5);
    expect(narrative.scenes[0].title).toContain('Problem');
    expect(narrative.scenes[4].durationFrames).toBe(180);
  });
});

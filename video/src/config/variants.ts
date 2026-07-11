import {buildVariantNarrative, type Narrative, type TerminalEvent} from '../lib/timeline';

export type TerminalFlavor = 'claude-code' | 'codex-cli';

export type VideoVariant = {
  id: string;
  flavor: TerminalFlavor;
  narrative: Narrative;
  events: TerminalEvent[];
};

const claudeEvents: TerminalEvent[] = [
  {atFrame: 0, type: 'prompt', content: 'claude --mcp goldfish'},
  {atFrame: 28, type: 'system', content: 'Connected MCP server: goldfish (42 tools)'},
  {atFrame: 70, type: 'command', content: 'goldfish.create_workspace name=baseline_lstm goal="forecast weekly sales"'},
  {atFrame: 130, type: 'output', content: '✓ Workspace baseline_lstm mounted (copy-based, no .git in user space)'},
  {atFrame: 190, type: 'command', content: 'goldfish.write_pipeline file=pipeline.yaml stages=preprocess,train,evaluate'},
  {atFrame: 260, type: 'output', content: '✓ Pipeline validated (typed signals, no cycles, dataset checks passed)'},
  {atFrame: 340, type: 'command', content: 'goldfish.run workspace=baseline_lstm stage=train profile=h100-spot'},
  {atFrame: 420, type: 'system', content: 'sync → commit → version tag baseline_lstm-v7 complete'},
  {atFrame: 500, type: 'output', content: 'train: started on gce:a3-highgpu-1g (spot) | logs streaming...'},
  {atFrame: 620, type: 'output', content: 'train: val_loss=0.0842 | model saved: artifacts/model/'},
  {atFrame: 720, type: 'command', content: 'goldfish.get_signal_lineage workspace=baseline_lstm signal=model'},
  {atFrame: 800, type: 'output', content: 'lineage: preprocess.features.npy -> train.model/ (version baseline_lstm-v7)'},
  {atFrame: 900, type: 'command', content: 'goldfish.compare_versions workspace=baseline_lstm base=v5 target=v7'},
  {atFrame: 980, type: 'output', content: 'Δ metric: +3.2% accuracy | full provenance + reproducible replay ready'},
  {atFrame: 1060, type: 'system', content: 'Goldfish lets agents move fast without losing trust.'},
];

export const variants: Record<string, VideoVariant> = {
  default: {
    id: 'default',
    flavor: 'claude-code',
    narrative: buildVariantNarrative({
      terminalTitle: 'Claude Code • Goldfish MCP Session',
      problem: 'Agentic ML research breaks when workspaces are mutable, lineage is manual, and runs are not reproducible.',
      solution: 'Goldfish gives Claude Code isolated workspaces, immutable versions, typed pipelines, and backend-agnostic execution.',
      cta: 'Ship faster experiments with full provenance. Goldfish is the operating system for agentic ML research.',
    }),
    events: claudeEvents,
  },
  codexStyle: {
    id: 'codexStyle',
    flavor: 'codex-cli',
    narrative: buildVariantNarrative({
      terminalTitle: 'Codex CLI • Goldfish Runtime',
      problem: 'CLI-first agents still need trustworthy experiment state and reproducible outputs.',
      solution: 'Same Goldfish abstractions: workspaces, versions, signals, and pipelines regardless of terminal flavor.',
      cta: 'Swap terminal persona, keep the Goldfish engine and story.',
    }),
    events: claudeEvents,
  },
};

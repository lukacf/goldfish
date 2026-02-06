import React from 'react';
import {AbsoluteFill, interpolate, useCurrentFrame, useVideoConfig} from 'remotion';

import {variants} from '../config/variants';

import {TerminalSimulation} from './TerminalSimulation';

type GoldfishIntroVideoProps = {
  variant?: keyof typeof variants;
};

const bullets = [
  'Copy-based workspaces keep agent edits isolated',
  'Every run syncs + commits + versions for full provenance',
  'Typed signal lineage makes pipelines interpretable',
  'Run local Docker or GCE through one backend interface',
];

export const GoldfishIntroVideo: React.FC<GoldfishIntroVideoProps> = ({variant = 'default'}) => {
  const selected = variants[variant];
  const frame = useCurrentFrame();
  const {fps} = useVideoConfig();

  const sceneBoundaries = selected.narrative.scenes.reduce<number[]>((acc, scene) => {
    const prev = acc.length === 0 ? 0 : acc[acc.length - 1];
    acc.push(prev + scene.durationFrames);
    return acc;
  }, []);

  const currentSceneIndex = sceneBoundaries.findIndex((boundary) => frame < boundary);
  const sceneIndex = currentSceneIndex === -1 ? selected.narrative.scenes.length - 1 : currentSceneIndex;
  const scene = selected.narrative.scenes[sceneIndex];

  const introFade = interpolate(frame, [0, fps], [0, 1], {extrapolateRight: 'clamp'});

  return (
    <AbsoluteFill style={{background: 'radial-gradient(circle at top, #1e293b, #020617 55%)', padding: 48}}>
      <div style={{display: 'grid', gridTemplateColumns: '2fr 1fr', gap: 28, height: '100%'}}>
        <TerminalSimulation title={selected.narrative.terminalTitle} events={selected.events} flavor={selected.flavor} />

        <div style={{display: 'flex', flexDirection: 'column', gap: 20, opacity: introFade}}>
          <div style={{background: 'rgba(2,6,23,0.68)', border: '1px solid #334155', borderRadius: 14, padding: 20}}>
            <p style={{fontSize: 18, color: '#38bdf8', margin: 0, textTransform: 'uppercase'}}>Goldfish intro</p>
            <h1 style={{fontSize: 42, lineHeight: 1.1, color: '#e2e8f0', margin: '8px 0 12px'}}>{scene.title}</h1>
            <p style={{fontSize: 24, lineHeight: 1.4, color: '#cbd5e1', margin: 0}}>{scene.subtitle}</p>
          </div>

          <div style={{background: 'rgba(2,6,23,0.68)', border: '1px solid #334155', borderRadius: 14, padding: 20}}>
            <h2 style={{fontSize: 24, color: '#e2e8f0', margin: '0 0 10px'}}>Why Goldfish for agentic ML</h2>
            <ul style={{margin: 0, paddingLeft: 24, color: '#cbd5e1', fontSize: 20, lineHeight: 1.5}}>
              {bullets.map((bullet) => (
                <li key={bullet}>{bullet}</li>
              ))}
            </ul>
          </div>
        </div>
      </div>
    </AbsoluteFill>
  );
};

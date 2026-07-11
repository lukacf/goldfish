import React from 'react';
import {interpolate, useCurrentFrame} from 'remotion';

import type {TerminalFlavor} from '../config/variants';
import {buildTerminalFrames, type TerminalEvent} from '../lib/timeline';

const colorByType: Record<TerminalEvent['type'], string> = {
  prompt: '#7dd3fc',
  command: '#fde047',
  output: '#c4b5fd',
  system: '#4ade80',
};

const prefixByFlavor: Record<TerminalFlavor, string> = {
  'claude-code': 'claude@research',
  'codex-cli': 'codex@research',
};

type TerminalSimulationProps = {
  events: TerminalEvent[];
  flavor: TerminalFlavor;
  title: string;
};

export const TerminalSimulation: React.FC<TerminalSimulationProps> = ({events, flavor, title}) => {
  const frame = useCurrentFrame();
  const visible = buildTerminalFrames(events, frame);
  const opacity = interpolate(frame, [0, 20], [0, 1], {extrapolateRight: 'clamp'});

  return (
    <div
      style={{
        opacity,
        background: '#09090b',
        border: '1px solid #27272a',
        borderRadius: 14,
        boxShadow: '0 15px 40px rgba(0,0,0,0.45)',
        overflow: 'hidden',
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      <div style={{padding: '14px 18px', borderBottom: '1px solid #27272a', color: '#e4e4e7', fontFamily: 'monospace'}}>
        {title}
      </div>
      <div style={{padding: 18, fontFamily: 'monospace', fontSize: 23, lineHeight: 1.5, color: '#f4f4f5', flex: 1}}>
        {visible.slice(-12).map((event, idx) => (
          <div key={`${event.atFrame}-${idx}`} style={{display: 'flex', gap: 12}}>
            <span style={{color: '#71717a'}}>{prefixByFlavor[flavor]}$</span>
            <span style={{color: colorByType[event.type]}}>{event.content}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

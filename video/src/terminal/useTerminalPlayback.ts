import { useMemo } from "react";
import type { ScriptEvent, TerminalLine, TerminalState, TerminalTheme } from "./types";

/**
 * Resolves the computed duration (in frames) of a single script event.
 */
function eventDuration(event: ScriptEvent, theme: TerminalTheme): number {
  switch (event.type) {
    case "prompt": {
      const speed = event.typingSpeed ?? 2;
      const typingFrames = event.text.length * speed;
      return typingFrames + (event.holdAfter ?? 10);
    }
    case "output":
      return event.holdAfter ?? 15;
    case "spinner":
      return event.duration;
    case "result":
      return event.holdAfter ?? 15;
    case "header":
      return event.holdAfter ?? 20;
    case "pause":
      return event.duration;
    case "clear":
      return 1;
    default:
      return 0;
  }
}

interface EventTiming {
  event: ScriptEvent;
  startFrame: number;
  endFrame: number;
}

/**
 * Pre-computes the absolute frame ranges for each event in the script.
 */
function computeTimeline(
  script: ScriptEvent[],
  theme: TerminalTheme
): EventTiming[] {
  const timeline: EventTiming[] = [];
  let cursor = 0;
  for (const event of script) {
    const dur = eventDuration(event, theme);
    timeline.push({ event, startFrame: cursor, endFrame: cursor + dur });
    cursor += dur;
  }
  return timeline;
}

/**
 * React hook that computes the terminal state at the current frame.
 *
 * @param script - Array of script events to play back
 * @param currentFrame - The current Remotion frame (relative to scene start)
 * @param theme - The terminal theme (determines prompt string, spinner chars, etc.)
 * @returns The computed TerminalState for rendering
 */
export function useTerminalPlayback(
  script: ScriptEvent[],
  currentFrame: number,
  theme: TerminalTheme
): TerminalState {
  const timeline = useMemo(() => computeTimeline(script, theme), [script, theme]);

  return useMemo(() => {
    const lines: TerminalLine[] = [];
    let currentTyping: TerminalState["currentTyping"] = null;
    let activeSpinner: TerminalState["activeSpinner"] = null;
    let showCursor = true;

    for (const { event, startFrame, endFrame } of timeline) {
      if (currentFrame < startFrame) {
        // Haven't reached this event yet
        break;
      }

      const elapsed = currentFrame - startFrame;
      const isActive = currentFrame < endFrame;

      switch (event.type) {
        case "header": {
          // Header lines appear instantly
          lines.push(...event.lines);
          break;
        }

        case "prompt": {
          const speed = event.typingSpeed ?? 2;
          const totalChars = event.text.length;
          const typedChars = Math.min(
            Math.floor(elapsed / speed),
            totalChars
          );

          if (isActive && typedChars < totalChars) {
            // Still typing
            currentTyping = {
              prompt: theme.promptString,
              text: event.text,
              visibleChars: typedChars,
            };
            showCursor = true;
          } else {
            // Typing complete — add as a finished line
            lines.push({
              spans: [
                { text: theme.promptString, color: theme.promptColor, bold: true },
                { text: event.text, color: theme.text },
              ],
            });
            currentTyping = null;
          }
          break;
        }

        case "output": {
          lines.push(...event.lines);
          break;
        }

        case "spinner": {
          if (isActive) {
            const spinChars = theme.spinnerChars;
            // Cycle spinner every 4 frames
            const charIndex = Math.floor(elapsed / 4) % spinChars.length;
            activeSpinner = {
              text: event.text,
              charIndex,
            };
            showCursor = false;
          } else {
            activeSpinner = null;
          }
          break;
        }

        case "result": {
          lines.push(...event.lines);
          activeSpinner = null;
          break;
        }

        case "pause": {
          // No visual change, just time passing
          break;
        }

        case "clear": {
          lines.length = 0;
          currentTyping = null;
          activeSpinner = null;
          break;
        }
      }
    }

    // Cursor blink: visible for 20 frames, hidden for 10 (repeat)
    if (showCursor) {
      const blinkCycle = currentFrame % 30;
      showCursor = blinkCycle < 20;
    }

    return { lines, currentTyping, activeSpinner, showCursor };
  }, [timeline, currentFrame, theme]);
}

/**
 * Terminal simulation types.
 *
 * This module defines the data structures for the terminal playback system.
 * The design is intentionally generic — themes and scripts are separate
 * concerns so the terminal can simulate any TUI tool.
 */

/** A single styled span within a terminal line */
export interface TextSpan {
  text: string;
  color?: string;
  bold?: boolean;
  dim?: boolean;
}

/** A complete terminal line (may contain multiple styled spans) */
export interface TerminalLine {
  spans: TextSpan[];
}

/**
 * Terminal script events — the building blocks of a terminal demo.
 *
 * Events are played back sequentially. Each event has a `duration` in frames
 * which determines how long it occupies before the next event starts.
 */
export type ScriptEvent =
  | {
      type: "prompt";
      /** The text the "user" types */
      text: string;
      /** Frames per character (lower = faster typing). Default: 2 */
      typingSpeed?: number;
      /** Extra frames to hold after typing finishes */
      holdAfter?: number;
    }
  | {
      type: "output";
      /** Lines to display (appear instantly) */
      lines: TerminalLine[];
      /** Frames to hold after displaying */
      holdAfter?: number;
    }
  | {
      type: "spinner";
      /** Text shown next to the spinner */
      text: string;
      /** How long the spinner runs (frames) */
      duration: number;
    }
  | {
      type: "result";
      /** Lines that replace the spinner */
      lines: TerminalLine[];
      /** Frames to hold after displaying */
      holdAfter?: number;
    }
  | {
      type: "header";
      /** Box-drawn header lines */
      lines: TerminalLine[];
      /** Frames to hold after displaying */
      holdAfter?: number;
    }
  | {
      type: "pause";
      duration: number;
    }
  | {
      type: "clear";
    };

/** Visual theme for the terminal */
export interface TerminalTheme {
  id: string;
  name: string;
  /** Window title shown in the chrome */
  windowTitle: string;
  /** Background color of the terminal body */
  bg: string;
  /** Header/title bar background */
  headerBg: string;
  /** Default text color */
  text: string;
  /** Dimmed text color */
  textDim: string;
  /** The prompt string shown before user input */
  promptString: string;
  /** Prompt color */
  promptColor: string;
  /** Cursor color */
  cursorColor: string;
  /** Font family */
  fontFamily: string;
  /** Font size in px */
  fontSize: number;
  /** Line height multiplier */
  lineHeight: number;
  /** Whether to show macOS-style traffic lights */
  showTrafficLights: boolean;
  /** Spinner characters (cycled through) */
  spinnerChars: string[];
  /** Spinner color */
  spinnerColor: string;
}

/**
 * Computed state of the terminal at a given frame.
 * Returned by useTerminalPlayback().
 */
export interface TerminalState {
  /** All completed lines in the scrollback */
  lines: TerminalLine[];
  /** Text currently being typed (null if not typing) */
  currentTyping: {
    prompt: string;
    text: string;
    visibleChars: number;
  } | null;
  /** Active spinner (null if not spinning) */
  activeSpinner: {
    text: string;
    charIndex: number;
  } | null;
  /** Whether the cursor should be visible */
  showCursor: boolean;
}

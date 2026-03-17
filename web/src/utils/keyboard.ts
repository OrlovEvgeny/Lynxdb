import { signal } from "@preact/signals";

/**
 * Central keyboard shortcut definitions, platform detection, and overlay signals.
 *
 * This module is the single source of truth for all keyboard shortcuts.
 * Components import SHORTCUTS for definitions, formatShortcut() for
 * platform-adaptive display, and the overlay signals for open/close state.
 */

const IS_MAC = /Mac|iPhone|iPad|iPod/.test(navigator.platform);

export type ShortcutDef = {
  key: string;
  mod?: boolean;
  shift?: boolean;
  alt?: boolean;
  label: string;
};

export const SHORTCUTS = {
  runQuery:      { key: "Enter", mod: true, label: "Run query" },
  focusEditor:   { key: "L", mod: true, label: "Focus editor" },
  toggleTail:    { key: "T", mod: true, shift: true, label: "Toggle live tail" },
  toggleSidebar: { key: "F", mod: true, shift: true, label: "Toggle sidebar" },
  openPalette:   { key: "K", mod: true, label: "Command palette" },
  closePanel:    { key: "Escape", label: "Close panel" },
  openHelp:      { key: "?", label: "Keyboard shortcuts" },
  focusSearch:   { key: "/", label: "Focus editor" },
  historyUp:     { key: "\u2191", mod: true, label: "Previous query" },
  historyDown:   { key: "\u2193", mod: true, label: "Next query" },
} as const;

/**
 * Format a shortcut definition into a platform-adaptive display string.
 *
 * On macOS: uses symbols (Cmd, Shift, Opt) joined without separators.
 * On other platforms: uses text (Ctrl, Shift, Alt) joined with "+".
 */
export function formatShortcut(def: ShortcutDef): string {
  const parts: string[] = [];
  if (def.mod) parts.push(IS_MAC ? "\u2318" : "Ctrl");
  if (def.shift) parts.push(IS_MAC ? "\u21E7" : "Shift");
  if (def.alt) parts.push(IS_MAC ? "\u2325" : "Alt");
  parts.push(def.key);
  return IS_MAC ? parts.join("") : parts.join("+");
}

/** Returns true if the current platform is macOS/iOS. */
export function isMac(): boolean {
  return IS_MAC;
}

/** Signal controlling the command palette open state. */
export const paletteOpen = signal(false);

/** Signal controlling the help overlay open state. */
export const helpOverlayOpen = signal(false);

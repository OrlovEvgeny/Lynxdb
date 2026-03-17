import { formatShortcut, SHORTCUTS } from "../utils/keyboard";
import styles from "./LiveTailButton.module.css";

interface LiveTailButtonProps {
  active: boolean;
  onToggle: () => void;
}

/**
 * Toggle button that activates / deactivates the live tail SSE stream.
 *
 * When active, renders with a green pulsing dot indicator.
 */
export function LiveTailButton({ active, onToggle }: LiveTailButtonProps) {
  return (
    <button
      type="button"
      class={`${styles.button} ${active ? styles.active : ""}`}
      onClick={onToggle}
      aria-pressed={active}
      aria-label={active ? "Stop live tail" : "Start live tail"}
      title={active
        ? `Stop live tail (${formatShortcut(SHORTCUTS.toggleTail)})`
        : `Start live tail (${formatShortcut(SHORTCUTS.toggleTail)})`}
    >
      {active && <span class={styles.dot} aria-hidden="true" />}
      Live Tail
    </button>
  );
}

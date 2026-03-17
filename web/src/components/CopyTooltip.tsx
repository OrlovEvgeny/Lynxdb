import styles from "./CopyTooltip.module.css";

interface CopyTooltipProps {
  visible: boolean;
  x: number;
  y: number;
}

export function CopyTooltip({ visible, x, y }: CopyTooltipProps) {
  if (!visible) return null;

  return (
    <div
      class={styles.tooltip}
      style={{ left: x, top: y - 28 }}
    >
      Copied!
    </div>
  );
}

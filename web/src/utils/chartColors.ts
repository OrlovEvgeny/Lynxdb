export const CHART_COLORS = [
  "#73bf69", "#5794f2", "#fade2a", "#ff9830",
  "#f2495c", "#b877d9", "#73bfb8", "#6e9fff",
];

export function cssVar(name: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

export function chartAxisFont(): string {
  return '11px Inter, "Helvetica Neue", Arial, sans-serif';
}

export function chartGridStroke(): string {
  return cssVar("--chart-grid") || "#2c3235";
}

export function chartAxisStroke(): string {
  return cssVar("--chart-axis-label") || "#8e8e8e";
}

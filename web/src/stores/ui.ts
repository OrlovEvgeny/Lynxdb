import { signal, effect } from "@preact/signals";

type Theme = "light" | "dark";

const STORAGE_KEY = "lynxdb_theme";

function getInitialTheme(): Theme {
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored === "light" || stored === "dark") return stored;
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

export const theme = signal<Theme>(getInitialTheme());

/** Apply theme class to <html> and persist to localStorage whenever signal changes. */
effect(() => {
  const t = theme.value;
  document.documentElement.classList.toggle("dark", t === "dark");
  localStorage.setItem(STORAGE_KEY, t);
});

export function toggleTheme(): void {
  theme.value = theme.value === "light" ? "dark" : "light";
}

/**
 * Follow OS theme changes when the user has not explicitly set a preference.
 * If localStorage has no stored value, match the system theme automatically.
 */
const mq = window.matchMedia("(prefers-color-scheme: dark)");
mq.addEventListener("change", (e) => {
  // Only auto-follow if no explicit user preference is stored
  const stored = localStorage.getItem(STORAGE_KEY);
  if (!stored) {
    theme.value = e.matches ? "dark" : "light";
  }
});

import { EditorView } from "@codemirror/view";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { tags } from "@lezer/highlight";

export const lynxTheme = EditorView.theme({
  "&": {
    backgroundColor: "var(--bg-secondary)",
    color: "var(--text-primary)",
    fontSize: "14px",
    fontFamily: "var(--font-mono)",
  },
  ".cm-content": {
    caretColor: "var(--accent)",
    padding: "8px 12px",
  },
  ".cm-cursor": {
    borderLeftColor: "var(--accent)",
  },
  "&.cm-focused .cm-selectionBackground, .cm-selectionBackground": {
    backgroundColor: "rgba(88, 166, 255, 0.2)",
  },
  ".cm-activeLine": {
    backgroundColor: "transparent",
  },
  ".cm-gutters": {
    display: "none",
  },
  "&.cm-focused": {
    outline: "1px solid var(--accent)",
    borderRadius: "var(--radius)",
  },
  ".cm-placeholder": {
    color: "var(--text-muted)",
  },
  /* Autocomplete tooltip styling */
  ".cm-tooltip.cm-tooltip-autocomplete": {
    backgroundColor: "var(--bg-secondary)",
    border: "1px solid var(--border)",
    borderRadius: "var(--radius)",
    boxShadow: "0 4px 12px rgba(0, 0, 0, 0.4)",
    overflow: "hidden",
  },
  ".cm-tooltip-autocomplete ul": {
    fontFamily: "var(--font-mono)",
    fontSize: "13px",
  },
  ".cm-tooltip-autocomplete ul li": {
    padding: "3px 8px",
    color: "var(--text-primary)",
  },
  ".cm-tooltip-autocomplete ul li[aria-selected]": {
    backgroundColor: "var(--bg-hover)",
    color: "var(--text-primary)",
  },
  ".cm-completionLabel": {
    color: "var(--text-primary)",
  },
  ".cm-completionDetail": {
    color: "var(--text-muted)",
    fontStyle: "normal",
    marginLeft: "8px",
  },
  ".cm-completionIcon": {
    opacity: "0.6",
  },
}, { dark: true });

export const lynxHighlighting = syntaxHighlighting(HighlightStyle.define([
  { tag: tags.keyword, color: "#79c0ff" },
  { tag: tags.definitionKeyword, color: "#79c0ff" },
  { tag: tags.function(tags.variableName), color: "#d2a8ff" },
  { tag: tags.operator, color: "#ff7b72" },
  { tag: tags.string, color: "#a5d6ff" },
  { tag: tags.number, color: "#79c0ff" },
  { tag: tags.bool, color: "#ff7b72" },
  { tag: tags.comment, color: "var(--text-muted)", fontStyle: "italic" },
  { tag: tags.punctuation, color: "var(--text-secondary)" },
  { tag: tags.name, color: "var(--text-primary)" },
]));

import { EditorView } from "@codemirror/view";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { tags } from "@lezer/highlight";

export const lynxTheme = EditorView.theme({
  "&": {
    backgroundColor: "var(--bg-primary)",
    color: "var(--text-primary)",
    fontSize: "14px",
    fontFamily: "var(--font-mono)",
    maxHeight: "50vh",
  },
  ".cm-scroller": {
    overflow: "auto",
  },
  ".cm-content": {
    caretColor: "var(--accent)",
    padding: "8px 12px",
    minHeight: "20px",
  },
  ".cm-cursor": {
    borderLeftColor: "var(--accent)",
  },
  "&.cm-focused .cm-selectionBackground, .cm-selectionBackground": {
    backgroundColor: "rgba(79, 70, 229, 0.12)",
  },
  ".cm-activeLine": {
    backgroundColor: "transparent",
  },
  ".cm-gutters": {
    backgroundColor: "var(--bg-secondary)",
    borderRight: "1px solid var(--border)",
    color: "var(--text-muted)",
    fontSize: "12px",
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
    backgroundColor: "var(--bg-primary)",
    border: "1px solid var(--border)",
    borderRadius: "var(--radius)",
    boxShadow: "0 4px 12px rgba(0, 0, 0, 0.1)",
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
  /* Completion icon styling: colored circle-dot per type */
  ".cm-completionIcon": {
    fontSize: "0",
    width: "16px",
    height: "16px",
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    marginRight: "4px",
    opacity: "1",
  },
  ".cm-completionIcon::after": {
    content: '""',
    display: "block",
    width: "8px",
    height: "8px",
    borderRadius: "50%",
  },
  ".cm-completionIcon-keyword::after": { backgroundColor: "#4F46E5" },   // indigo -- commands
  ".cm-completionIcon-property::after": { backgroundColor: "#0d9488" },   // teal -- fields
  ".cm-completionIcon-function::after": { backgroundColor: "#7c3aed" },   // purple -- functions
  ".cm-completionIcon-text::after": { backgroundColor: "#6b7280" },       // gray -- values
  ".cm-completionIcon-variable::after": { backgroundColor: "#2563eb" },   // blue -- indexes
}, { dark: false });

export const lynxHighlighting = syntaxHighlighting(HighlightStyle.define([
  { tag: tags.keyword, color: "#4F46E5" },
  { tag: tags.definitionKeyword, color: "#4F46E5" },
  { tag: tags.function(tags.variableName), color: "#7c3aed" },
  { tag: tags.operator, color: "#c4432b" },
  { tag: tags.string, color: "#2563eb" },
  { tag: tags.number, color: "#0d9488" },
  { tag: tags.bool, color: "#c4432b" },
  { tag: tags.comment, color: "var(--text-muted)", fontStyle: "italic" },
  { tag: tags.punctuation, color: "var(--text-secondary)" },
  { tag: tags.name, color: "var(--text-primary)" },
]));

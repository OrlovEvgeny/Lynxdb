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
    padding: "6px 12px",
    minHeight: "18px",
  },
  ".cm-cursor": {
    borderLeftColor: "var(--accent)",
  },
  "&.cm-focused .cm-selectionBackground, .cm-selectionBackground": {
    backgroundColor: "rgba(50, 116, 217, 0.14)",
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
    outline: "none",
  },
  ".cm-placeholder": {
    color: "var(--text-muted)",
  },
  /* Autocomplete tooltip styling */
  ".cm-tooltip.cm-tooltip-autocomplete": {
    backgroundColor: "var(--bg-primary)",
    border: "1px solid var(--border)",
    borderRadius: "var(--radius)",
    boxShadow: "none",
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
  ".cm-completionIcon-keyword::after": { backgroundColor: "#3274d9" },   // blue -- commands
  ".cm-completionIcon-property::after": { backgroundColor: "#73bf69" },   // green -- fields
  ".cm-completionIcon-function::after": { backgroundColor: "#b877d9" },   // purple -- functions
  ".cm-completionIcon-text::after": { backgroundColor: "#8e8e8e" },       // gray -- values
  ".cm-completionIcon-variable::after": { backgroundColor: "#5794f2" },   // blue -- indexes
  /* Diagnostic (lint) styling for syntax error underlines and tooltips */
  ".cm-diagnostic-error": {
    borderBottom: "2px solid #f2495c",
    paddingBottom: "1px",
  },
  ".cm-tooltip-lint": {
    backgroundColor: "var(--bg-primary)",
    border: "1px solid var(--border)",
    borderRadius: "var(--radius)",
    padding: "4px 8px",
    fontSize: "13px",
    color: "var(--text-primary)",
  },
}, { dark: false });

export const lynxHighlighting = syntaxHighlighting(HighlightStyle.define([
  { tag: tags.keyword, color: "#3274d9" },
  { tag: tags.definitionKeyword, color: "#3274d9" },
  { tag: tags.function(tags.variableName), color: "#b877d9" },
  { tag: tags.operator, color: "#f2495c" },
  { tag: tags.string, color: "#5794f2" },
  { tag: tags.number, color: "#73bf69" },
  { tag: tags.bool, color: "#f2495c" },
  { tag: tags.comment, color: "var(--text-muted)", fontStyle: "italic" },
  { tag: tags.punctuation, color: "var(--text-secondary)" },
  { tag: tags.name, color: "var(--text-primary)" },
]));

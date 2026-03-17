import { useRef, useEffect, useCallback, useState } from "preact/hooks";
import { signal } from "@preact/signals";
import type { Signal } from "@preact/signals";
import {
  PRESETS,
  getTimeRangeLabel,
  toNowExpr,
  parseNowExpression,
} from "../utils/timeFormat";
import styles from "./TimeRangePicker.module.css";

interface TimeRangePickerProps {
  from: Signal<string>;
  to: Signal<string | undefined>;
  onApply?: () => void;
}

const open = signal(false);

export function TimeRangePicker({ from, to, onApply }: TimeRangePickerProps) {
  const wrapperRef = useRef<HTMLDivElement>(null);
  const [fromInput, setFromInput] = useState("");
  const [toInput, setToInput] = useState("");
  const [quickSearch, setQuickSearch] = useState("");
  const [validationError, setValidationError] = useState<string | null>(null);

  // Sync inputs when dropdown opens
  useEffect(() => {
    if (open.value) {
      setFromInput(toNowExpr(from.value));
      setToInput(toNowExpr(to.value));
      setQuickSearch("");
      setValidationError(null);
    }
  }, [open.value]);

  // Apply absolute/relative inputs from left panel
  const handleApply = useCallback(() => {
    setValidationError(null);

    const parsedFrom = parseNowExpression(fromInput);
    const parsedTo = parseNowExpression(toInput);

    if (parsedFrom === null) {
      // Try as ISO date
      const d = new Date(fromInput);
      if (isNaN(d.getTime())) {
        setValidationError("Invalid From value. Use now-3h or ISO date.");
        return;
      }
      from.value = d.toISOString();
    } else if (parsedFrom === undefined) {
      // "now" as from doesn't make sense, but allow it
      setValidationError("From cannot be 'now'. Use a relative offset like now-1h.");
      return;
    } else {
      from.value = parsedFrom;
    }

    if (parsedTo === null) {
      const d = new Date(toInput);
      if (isNaN(d.getTime())) {
        setValidationError("Invalid To value. Use now or now-30m or ISO date.");
        return;
      }
      to.value = d.toISOString();
    } else if (parsedTo === undefined) {
      to.value = undefined;
    } else {
      to.value = parsedTo;
    }

    open.value = false;
    onApply?.();
  }, [from, to, onApply, fromInput, toInput]);

  // Click a quick-range preset
  const handlePreset = useCallback(
    (value: string) => {
      from.value = value;
      to.value = undefined;
      open.value = false;
      onApply?.();
    },
    [from, to, onApply],
  );

  // Close on outside click
  useEffect(() => {
    function onPointerDown(e: PointerEvent) {
      if (
        wrapperRef.current &&
        !wrapperRef.current.contains(e.target as Node)
      ) {
        open.value = false;
      }
    }
    document.addEventListener("pointerdown", onPointerDown, true);
    return () =>
      document.removeEventListener("pointerdown", onPointerDown, true);
  }, []);

  // Close on Escape
  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape" && open.value) {
        open.value = false;
      }
    }
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);

  // Filter presets by search
  const filteredPresets = quickSearch
    ? PRESETS.filter((p) =>
        p.label.toLowerCase().includes(quickSearch.toLowerCase()),
      )
    : PRESETS;

  // Determine which preset is active
  const activePreset =
    (to.value === undefined || to.value === "now")
      ? PRESETS.find((p) => p.value === from.value)?.value ?? null
      : null;

  return (
    <div class={styles.wrapper} ref={wrapperRef}>
      <button
        type="button"
        class={styles.trigger}
        onClick={() => {
          open.value = !open.value;
        }}
        aria-haspopup="dialog"
        aria-expanded={open.value}
      >
        <svg class={styles.triggerIcon} viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="7" cy="7" r="5.5" /><path d="M7 4.5V7l2 1.5" /></svg>
        {getTimeRangeLabel(from.value, to.value)}
      </button>

      {open.value && (
        <div class={styles.dropdown} role="dialog" aria-label="Time range picker">
          {/* Left panel: absolute time range */}
          <div class={styles.leftPanel}>
            <div class={styles.panelTitle}>Absolute time range</div>

            <div class={styles.inputGroup}>
              <label class={styles.inputLabel}>From</label>
              <input
                type="text"
                class={styles.textInput}
                value={fromInput}
                placeholder="now-1h"
                onInput={(e) => {
                  setFromInput((e.target as HTMLInputElement).value);
                  setValidationError(null);
                }}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    handleApply();
                  }
                }}
              />
            </div>

            <div class={styles.inputGroup}>
              <label class={styles.inputLabel}>To</label>
              <input
                type="text"
                class={styles.textInput}
                value={toInput}
                placeholder="now"
                onInput={(e) => {
                  setToInput((e.target as HTMLInputElement).value);
                  setValidationError(null);
                }}
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
                    e.preventDefault();
                    handleApply();
                  }
                }}
              />
            </div>

            {validationError && (
              <div class={styles.validationError}>{validationError}</div>
            )}

            <button
              type="button"
              class={styles.applyBtn}
              onClick={handleApply}
            >
              Apply time range
            </button>
          </div>

          {/* Right panel: quick ranges */}
          <div class={styles.rightPanel}>
            <input
              type="text"
              class={styles.searchInput}
              placeholder="Search quick ranges"
              value={quickSearch}
              onInput={(e) =>
                setQuickSearch((e.target as HTMLInputElement).value)
              }
            />
            <div class={styles.presetList}>
              {filteredPresets.map((preset) => (
                <button
                  key={preset.value}
                  type="button"
                  class={`${styles.presetItem} ${activePreset === preset.value ? styles.presetItemActive : ""}`}
                  onClick={() => handlePreset(preset.value)}
                >
                  {preset.label}
                </button>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

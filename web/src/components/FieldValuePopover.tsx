import { useState, useEffect, useRef, useCallback } from "preact/hooks";
import { fetchFieldValues } from "../api/client";
import type { FieldValue } from "../api/client";
import styles from "./FieldValuePopover.module.css";

interface FieldValuePopoverProps {
  fieldName: string;
  anchorRect: DOMRect;
  onFilter: (field: string, value: string, exclude: boolean) => void;
  onClose: () => void;
}

const POPOVER_WIDTH = 280;
const POPOVER_MAX_HEIGHT = 360;

function computePosition(anchorRect: DOMRect): { top: number; left: number } {
  const viewportWidth = window.innerWidth;
  const viewportHeight = window.innerHeight;

  let left = anchorRect.right + 8;
  let top = anchorRect.top;

  // If overflows right edge, position to the left of anchor
  if (left + POPOVER_WIDTH > viewportWidth) {
    left = anchorRect.left - POPOVER_WIDTH - 8;
  }

  // If overflows bottom, shift up
  if (top + POPOVER_MAX_HEIGHT > viewportHeight) {
    top = viewportHeight - POPOVER_MAX_HEIGHT - 8;
  }

  // Ensure top is never negative
  if (top < 8) top = 8;

  return { top, left };
}

export function FieldValuePopover({
  fieldName,
  anchorRect,
  onFilter,
  onClose,
}: FieldValuePopoverProps) {
  const [values, setValues] = useState<FieldValue[]>([]);
  const [loading, setLoading] = useState(true);
  const [fetchError, setFetchError] = useState(false);
  const popoverRef = useRef<HTMLDivElement>(null);

  // Fetch top 10 values on mount
  useEffect(() => {
    setLoading(true);
    setFetchError(false);
    fetchFieldValues(fieldName, 10)
      .then((vals) => {
        setValues(vals);
        setLoading(false);
      })
      .catch(() => {
        setFetchError(true);
        setLoading(false);
      });
  }, [fieldName]);

  // Click-outside detection
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        onClose();
      }
    }
    // Delay listener attachment to avoid immediately closing on the triggering click
    const timer = setTimeout(() => {
      document.addEventListener("click", handleClick, true);
    }, 0);
    return () => {
      clearTimeout(timer);
      document.removeEventListener("click", handleClick, true);
    };
  }, [onClose]);

  // Escape key closes popover
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        onClose();
      }
    }
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  const handleInclude = useCallback(
    (value: string) => {
      onFilter(fieldName, value, false);
    },
    [fieldName, onFilter],
  );

  const handleExclude = useCallback(
    (value: string) => {
      onFilter(fieldName, value, true);
    },
    [fieldName, onFilter],
  );

  const pos = computePosition(anchorRect);
  const maxCount = values.length > 0 ? Math.max(...values.map((v) => v.count)) : 1;

  return (
    <div
      ref={popoverRef}
      class={styles.popover}
      style={{
        top: `${pos.top}px`,
        left: `${pos.left}px`,
      }}
    >
      <div class={styles.popoverHeader}>
        {fieldName}
        <div class={styles.popoverSubtitle}>Top 10 values</div>
      </div>

      {loading && (
        <div class={styles.loadingState}>Loading...</div>
      )}

      {fetchError && (
        <div class={styles.emptyState}>Failed to load values</div>
      )}

      {!loading && !fetchError && values.length === 0 && (
        <div class={styles.emptyState}>No values found</div>
      )}

      {!loading && !fetchError && values.length > 0 && (
        <div class={styles.valuesList}>
          {values.map((v) => {
            const pct = maxCount > 0 ? (v.count / maxCount) * 100 : 0;
            return (
              <div class={styles.valueRow} key={v.value}>
                <span class={styles.valueName} title={v.value}>
                  {v.value}
                </span>
                <div class={styles.valueBarContainer}>
                  <div
                    class={styles.valueBar}
                    style={{ width: `${pct}%` }}
                  />
                </div>
                <span class={styles.valueCount}>{v.count}</span>
                <div class={styles.filterBtns}>
                  <button
                    type="button"
                    class={styles.addBtn}
                    onClick={() => handleInclude(v.value)}
                    title={`Add filter: ${fieldName}="${v.value}"`}
                    aria-label={`Include ${v.value}`}
                  >
                    +
                  </button>
                  <button
                    type="button"
                    class={styles.excludeBtn}
                    onClick={() => handleExclude(v.value)}
                    title={`Exclude: ${fieldName}!="${v.value}"`}
                    aria-label={`Exclude ${v.value}`}
                  >
                    -
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

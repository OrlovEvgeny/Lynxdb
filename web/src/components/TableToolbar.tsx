import { useState, useEffect, useCallback, useRef } from "preact/hooks";
import { Table2, List, Download } from "lucide-preact";
import styles from "./TableToolbar.module.css";

interface TableToolbarProps {
  viewMode: "table" | "list";
  onViewModeChange: (mode: "table" | "list") => void;
  onExport: (format: "csv" | "json", scope: "page" | "all") => void;
  totalCount: number;
  pageCount: number;
}

const fmtNum = (n: number) => new Intl.NumberFormat().format(n);

export function TableToolbar({
  viewMode,
  onViewModeChange,
  onExport,
  totalCount,
  pageCount,
}: TableToolbarProps) {
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Close dropdown on outside click
  useEffect(() => {
    if (!dropdownOpen) return;
    function onPointerDown(e: PointerEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setDropdownOpen(false);
      }
    }
    document.addEventListener("pointerdown", onPointerDown, true);
    return () => document.removeEventListener("pointerdown", onPointerDown, true);
  }, [dropdownOpen]);

  const handleExportClick = useCallback(
    (format: "csv" | "json", scope: "page" | "all") => {
      setDropdownOpen(false);
      onExport(format, scope);
    },
    [onExport],
  );

  return (
    <div class={styles.toolbar}>
      <div class={styles.left}>
        {/* View mode segmented control */}
        <div class={styles.segmented}>
          <button
            type="button"
            class={`${styles.segBtn} ${viewMode === "table" ? styles.segBtnActive : ""}`}
            onClick={() => onViewModeChange("table")}
            title="Table view"
            aria-label="Table view"
          >
            <Table2 size={14} />
          </button>
          <button
            type="button"
            class={`${styles.segBtn} ${viewMode === "list" ? styles.segBtnActive : ""}`}
            onClick={() => onViewModeChange("list")}
            title="List view"
            aria-label="List view"
          >
            <List size={14} />
          </button>
        </div>
      </div>

      <div class={styles.right}>
        {/* Export dropdown */}
        <div class={styles.exportWrapper} ref={dropdownRef}>
          <button
            type="button"
            class={styles.exportBtn}
            onClick={() => setDropdownOpen(!dropdownOpen)}
            aria-haspopup="menu"
            aria-expanded={dropdownOpen}
          >
            <Download size={14} />
            Export
          </button>
          {dropdownOpen && (
            <div class={styles.exportDropdown} role="menu">
              <button
                type="button"
                class={styles.exportOption}
                role="menuitem"
                onClick={() => handleExportClick("csv", "page")}
              >
                CSV - Current page ({fmtNum(pageCount)} rows)
              </button>
              <button
                type="button"
                class={styles.exportOption}
                role="menuitem"
                onClick={() => handleExportClick("csv", "all")}
              >
                CSV - All results ({fmtNum(totalCount)} rows)
              </button>
              <button
                type="button"
                class={styles.exportOption}
                role="menuitem"
                onClick={() => handleExportClick("json", "page")}
              >
                JSON - Current page ({fmtNum(pageCount)} rows)
              </button>
              <button
                type="button"
                class={styles.exportOption}
                role="menuitem"
                onClick={() => handleExportClick("json", "all")}
              >
                JSON - All results ({fmtNum(totalCount)} rows)
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

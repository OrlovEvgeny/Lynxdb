import { useMemo } from "preact/hooks";
import styles from "./PaginationBar.module.css";

interface PaginationBarProps {
  page: number;
  pageSize: number;
  total: number;
  onPageChange: (page: number) => void;
  onPageSizeChange: (size: number) => void;
}

const PAGE_SIZES = [50, 100, 500, 1000];
const fmtNum = (n: number) => new Intl.NumberFormat().format(n);

/**
 * Compute which page numbers to display. Always shows first, last,
 * and 2 pages around the current page, with "..." for gaps.
 */
function computePageNumbers(current: number, total: number): (number | "...")[] {
  if (total <= 7) {
    return Array.from({ length: total }, (_, i) => i + 1);
  }

  const pages = new Set<number>();
  pages.add(1);
  pages.add(total);
  for (let i = Math.max(2, current - 1); i <= Math.min(total - 1, current + 1); i++) {
    pages.add(i);
  }

  const sorted = Array.from(pages).sort((a, b) => a - b);
  const result: (number | "...")[] = [];

  for (let i = 0; i < sorted.length; i++) {
    if (i > 0 && sorted[i] - sorted[i - 1] > 1) {
      result.push("...");
    }
    result.push(sorted[i]);
  }

  return result;
}

export function PaginationBar({
  page,
  pageSize,
  total,
  onPageChange,
  onPageSizeChange,
}: PaginationBarProps) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const pageNumbers = useMemo(() => computePageNumbers(page, totalPages), [page, totalPages]);

  return (
    <div class={styles.bar}>
      <div class={styles.left}>
        <span class={styles.totalCount}>{fmtNum(total)} results</span>
      </div>

      <div class={styles.center}>
        <button
          type="button"
          class={styles.navBtn}
          disabled={page <= 1}
          onClick={() => onPageChange(page - 1)}
          aria-label="Previous page"
        >
          &lsaquo; Prev
        </button>

        {pageNumbers.map((item, idx) =>
          item === "..." ? (
            <span key={`ellipsis-${idx}`} class={styles.ellipsis}>
              ...
            </span>
          ) : (
            <button
              key={item}
              type="button"
              class={`${styles.pageBtn} ${item === page ? styles.pageBtnActive : ""}`}
              onClick={() => onPageChange(item)}
              aria-label={`Page ${item}`}
              aria-current={item === page ? "page" : undefined}
            >
              {item}
            </button>
          ),
        )}

        <button
          type="button"
          class={styles.navBtn}
          disabled={page >= totalPages}
          onClick={() => onPageChange(page + 1)}
          aria-label="Next page"
        >
          Next &rsaquo;
        </button>
      </div>

      <div class={styles.right}>
        <select
          class={styles.sizeSelect}
          value={pageSize}
          onChange={(e) => onPageSizeChange(Number((e.target as HTMLSelectElement).value))}
          aria-label="Page size"
        >
          {PAGE_SIZES.map((size) => (
            <option key={size} value={size}>
              {size} / page
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}

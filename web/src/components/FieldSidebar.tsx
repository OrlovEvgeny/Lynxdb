import { useState, useCallback } from "preact/hooks";
import { fetchFieldValues } from "../api/client";
import type { FieldInfo, FieldValue } from "../api/client";
import { formatCount } from "../utils/format";
import styles from "./FieldSidebar.module.css";

interface FieldSidebarProps {
  visible: boolean;
  fields: FieldInfo[];
  onToggle: () => void;
  onFieldFilter: (field: string, value: string) => void;
}

export function FieldSidebar({ visible, fields, onToggle, onFieldFilter }: FieldSidebarProps) {
  const [expandedField, setExpandedField] = useState<string | null>(null);
  const [fieldValues, setFieldValues] = useState<Map<string, FieldValue[]>>(new Map());
  const [searchFilter, setSearchFilter] = useState("");
  const [loadingValues, setLoadingValues] = useState<string | null>(null);

  const handleToggleField = useCallback(async (fieldName: string) => {
    if (expandedField === fieldName) {
      setExpandedField(null);
      return;
    }

    setExpandedField(fieldName);

    // Lazy-load values if not already cached
    if (!fieldValues.has(fieldName)) {
      setLoadingValues(fieldName);
      try {
        const values = await fetchFieldValues(fieldName, 10);
        setFieldValues((prev) => {
          const next = new Map(prev);
          next.set(fieldName, values);
          return next;
        });
      } catch {
        // On failure, set empty array so we don't retry endlessly
        setFieldValues((prev) => {
          const next = new Map(prev);
          next.set(fieldName, []);
          return next;
        });
      } finally {
        setLoadingValues(null);
      }
    }
  }, [expandedField, fieldValues]);

  const handleValueClick = useCallback((field: string, value: string) => {
    onFieldFilter(field, value);
  }, [onFieldFilter]);

  const handleSearchChange = useCallback((e: Event) => {
    const target = e.target as HTMLInputElement;
    setSearchFilter(target.value);
  }, []);

  // Filter fields by search term
  const normalizedFilter = searchFilter.toLowerCase();
  const filteredFields = normalizedFilter
    ? fields.filter((f) => f.name.toLowerCase().includes(normalizedFilter))
    : fields;

  if (!visible) {
    return (
      <button
        type="button"
        class={styles.collapsedToggle}
        onClick={onToggle}
        aria-label="Show fields sidebar"
        title="Show fields"
      >
        &#9656;
      </button>
    );
  }

  return (
    <aside class={styles.sidebar} aria-label="Fields">
      <button
        type="button"
        class={styles.toggleBtn}
        onClick={onToggle}
        aria-label="Hide fields sidebar"
        title="Hide fields"
      >
        &#9666;
      </button>

      <div class={styles.header}>
        <span class={styles.headerTitle}>Fields</span>
      </div>

      <input
        type="text"
        class={styles.searchInput}
        placeholder="Filter fields..."
        value={searchFilter}
        onInput={handleSearchChange}
        aria-label="Filter fields"
      />

      <div class={styles.fieldList}>
        {filteredFields.length === 0 && fields.length > 0 && (
          <div class={styles.emptyState}>No matching fields</div>
        )}
        {fields.length === 0 && (
          <div class={styles.emptyState}>
            Run a query to see fields
          </div>
        )}
        {filteredFields.map((field) => {
          const isExpanded = expandedField === field.name;
          const values = fieldValues.get(field.name);
          const isLoading = loadingValues === field.name;
          const maxValueCount = values
            ? Math.max(...values.map((v) => v.count), 1)
            : 1;

          return (
            <div key={field.name} class={styles.fieldItem}>
              <button
                type="button"
                class={`${styles.fieldRow} ${isExpanded ? styles.fieldRowExpanded : ""}`}
                onClick={() => handleToggleField(field.name)}
                aria-expanded={isExpanded}
              >
                <span
                  class={`${styles.fieldIcon} ${isExpanded ? styles.fieldIconExpanded : ""}`}
                  aria-hidden="true"
                >
                  &#9656;
                </span>
                <span class={styles.fieldName}>{field.name}</span>
                <span class={styles.fieldType}>{field.type}</span>
                <span class={styles.fieldCount}>{formatCount(field.count)}</span>
              </button>

              {isExpanded && (
                <div class={styles.valuesPanel}>
                  {isLoading && (
                    <div class={styles.valuesLoading}>Loading...</div>
                  )}
                  {!isLoading && values && values.length === 0 && (
                    <div class={styles.valuesLoading}>No values</div>
                  )}
                  {!isLoading && values && values.map((val) => {
                    const barWidth = (val.count / maxValueCount) * 100;
                    return (
                      <button
                        key={val.value}
                        type="button"
                        class={styles.valueRow}
                        onClick={() => handleValueClick(field.name, val.value)}
                        title={`Filter: ${field.name}="${val.value}"`}
                      >
                        <span
                          class={styles.valueBar}
                          style={{ inlineSize: `${barWidth}%` }}
                          aria-hidden="true"
                        />
                        <span class={styles.valueLabel}>{val.value}</span>
                        <span class={styles.valueCount}>{formatCount(val.count)}</span>
                      </button>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </aside>
  );
}

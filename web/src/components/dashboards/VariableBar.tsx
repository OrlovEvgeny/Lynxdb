import { useState, useEffect } from "preact/hooks";
import type { DashboardVariable } from "../../api/client";
import { fetchFieldValues } from "../../api/client";
import styles from "./VariableBar.module.css";

interface VariableBarProps {
  variables: DashboardVariable[];
  values: Record<string, string>;
  onChange: (name: string, value: string) => void;
}

interface FieldOptions {
  loading: boolean;
  options: string[];
}

export function VariableBar({ variables, values, onChange }: VariableBarProps) {
  // Track loaded field values for field_values type variables
  const [fieldOptions, setFieldOptions] = useState<Record<string, FieldOptions>>({});

  // Fetch field values for field_values type variables
  useEffect(() => {
    for (const v of variables) {
      if (v.type !== "field_values") continue;
      if (fieldOptions[v.name] && !fieldOptions[v.name].loading) continue;

      setFieldOptions((prev) => ({
        ...prev,
        [v.name]: { loading: true, options: [] },
      }));

      fetchFieldValues(v.field, 50)
        .then((vals) => {
          setFieldOptions((prev) => ({
            ...prev,
            [v.name]: { loading: false, options: vals.map((fv) => fv.value) },
          }));
        })
        .catch(() => {
          setFieldOptions((prev) => ({
            ...prev,
            [v.name]: { loading: false, options: [] },
          }));
        });
    }
  }, [variables]);

  if (variables.length === 0) return null;

  return (
    <div class={styles.bar}>
      {variables.map((v) => {
        const selected = values[v.name] ?? v.default ?? "*";
        let options: string[] = [];

        if (v.type === "field_values") {
          const fo = fieldOptions[v.name];
          if (fo && !fo.loading) {
            options = fo.options;
          }
        } else if (v.type === "custom") {
          // field contains comma-separated values
          options = v.field
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean);
        }

        const isLoading = v.type === "field_values" && fieldOptions[v.name]?.loading;

        return (
          <div key={v.name} class={styles.varGroup}>
            <span class={styles.varLabel}>{v.label || v.name}</span>
            <select
              class={styles.varSelect}
              value={selected}
              onChange={(e) =>
                onChange(v.name, (e.target as HTMLSelectElement).value)
              }
              disabled={isLoading}
            >
              <option value="*">All</option>
              {isLoading && <option disabled>Loading...</option>}
              {options.map((opt) => (
                <option key={opt} value={opt}>
                  {opt}
                </option>
              ))}
            </select>
          </div>
        );
      })}
    </div>
  );
}

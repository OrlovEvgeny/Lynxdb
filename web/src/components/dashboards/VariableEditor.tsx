import type { DashboardVariable } from "../../api/client";
import styles from "./VariableEditor.module.css";

interface VariableEditorProps {
  variables: DashboardVariable[];
  onChange: (variables: DashboardVariable[]) => void;
}

export function VariableEditor({ variables, onChange }: VariableEditorProps) {
  function handleUpdate(index: number, field: keyof DashboardVariable, value: string) {
    const updated = [...variables];
    updated[index] = { ...updated[index], [field]: value } as DashboardVariable;
    onChange(updated);
  }

  function handleTypeChange(index: number, newType: "field_values" | "custom") {
    const updated = [...variables];
    updated[index] = { ...updated[index], type: newType };
    onChange(updated);
  }

  function handleDelete(index: number) {
    onChange(variables.filter((_, i) => i !== index));
  }

  function handleAdd() {
    const count = variables.length + 1;
    onChange([
      ...variables,
      {
        name: `var${count}`,
        type: "field_values",
        field: "source",
        default: "*",
      },
    ]);
  }

  return (
    <div class={styles.section}>
      <div class={styles.sectionTitle}>Variables</div>

      {variables.length > 0 && (
        <div class={styles.headerRow}>
          <span>Name</span>
          <span>Type</span>
          <span>Field / Values</span>
          <span>Default</span>
          <span />
        </div>
      )}

      {variables.map((v, i) => (
        <div key={i} class={styles.varRow}>
          <input
            class={styles.varInput}
            type="text"
            value={v.name}
            onInput={(e) =>
              handleUpdate(i, "name", (e.target as HTMLInputElement).value)
            }
            placeholder="Variable name"
          />
          <select
            class={styles.varSelect}
            value={v.type}
            onChange={(e) =>
              handleTypeChange(
                i,
                (e.target as HTMLSelectElement).value as "field_values" | "custom",
              )
            }
          >
            <option value="field_values">Field Values</option>
            <option value="custom">Custom</option>
          </select>
          <input
            class={styles.varInput}
            type="text"
            value={v.field}
            onInput={(e) =>
              handleUpdate(i, "field", (e.target as HTMLInputElement).value)
            }
            placeholder={v.type === "field_values" ? "Field name" : "val1,val2,val3"}
          />
          <input
            class={styles.varInput}
            type="text"
            value={v.default ?? ""}
            onInput={(e) =>
              handleUpdate(i, "default", (e.target as HTMLInputElement).value)
            }
            placeholder="*"
          />
          <button
            type="button"
            class={styles.deleteBtn}
            onClick={() => handleDelete(i)}
            title="Remove variable"
          >
            x
          </button>
        </div>
      ))}

      <button type="button" class={styles.addBtn} onClick={handleAdd}>
        + Add Variable
      </button>
    </div>
  );
}

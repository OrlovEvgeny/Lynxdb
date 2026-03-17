import { useState } from "preact/hooks";
import type { PipelineStage, OptimizerRule } from "../api/client";
import styles from "./PipelineFlow.module.css";

interface PipelineFlowProps {
  stages: PipelineStage[];
  optimizerRules?: OptimizerRule[];
}

/** Maps optimizer rule names to the pipeline stage commands they typically affect. */
const RULE_STAGE_MAP: Record<string, string[]> = {
  predicate_pushdown: ["WHERE", "SEARCH"],
  column_pruning: ["SEARCH", "TABLE", "FIELDS"],
  constant_folding: ["EVAL", "WHERE"],
  bloom_filter_pruning: ["SEARCH"],
  time_range_pruning: ["SEARCH"],
  partial_aggregation: ["STATS"],
  topk_pushdown: ["SORT", "HEAD"],
  regex_literal_extraction: ["REX", "SEARCH"],
  mv_rewrite: ["FROM"],
  cse_elimination: ["EVAL"],
  join_optimization: ["JOIN"],
};

/**
 * Map optimizer rules to stage indexes. Returns a Map where key is the stage
 * index and value is the list of rules that affect that stage. Rules that
 * don't match any stage are omitted (shown in the Optimizer Rules tab instead).
 */
function mapRulesToStages(
  rules: OptimizerRule[],
  stages: PipelineStage[],
): Map<number, OptimizerRule[]> {
  const result = new Map<number, OptimizerRule[]>();

  for (const rule of rules) {
    const targets = RULE_STAGE_MAP[rule.name];
    if (!targets) continue;

    for (let i = 0; i < stages.length; i++) {
      const cmd = stages[i].command.toUpperCase();
      if (targets.includes(cmd)) {
        const existing = result.get(i) ?? [];
        existing.push(rule);
        result.set(i, existing);
      }
    }
  }

  return result;
}

export function PipelineFlow({ stages, optimizerRules }: PipelineFlowProps) {
  const [expandedStage, setExpandedStage] = useState<number | null>(null);

  const ruleMap = optimizerRules ? mapRulesToStages(optimizerRules, stages) : new Map();

  const handleStageClick = (index: number) => {
    setExpandedStage(expandedStage === index ? null : index);
  };

  return (
    <div>
      <div class={styles.pipelineRow}>
        {stages.map((stage, i) => {
          const stageRules = ruleMap.get(i);
          const isSelected = expandedStage === i;

          return (
            <>
              {i > 0 && (
                <span class={styles.arrow} aria-hidden="true">{"\u2192"}</span>
              )}
              <div
                class={`${styles.stageCard}${isSelected ? ` ${styles.stageCardActive}` : ""}`}
                onClick={() => handleStageClick(i)}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    handleStageClick(i);
                  }
                }}
                aria-expanded={isSelected}
                aria-label={`Pipeline stage: ${stage.command}`}
              >
                <span class={styles.stageName}>{stage.command}</span>
                {stage.description && (
                  <span class={styles.stageDesc} title={stage.description}>
                    {stage.description}
                  </span>
                )}
                {stageRules && stageRules.length > 0 && (
                  <span
                    class={styles.optimizerBadge}
                    title={stageRules.map((r: OptimizerRule) => r.name).join(", ")}
                    aria-label={`Optimizer: ${stageRules.map((r: OptimizerRule) => r.name).join(", ")}`}
                  />
                )}
              </div>
            </>
          );
        })}
      </div>

      {expandedStage !== null && stages[expandedStage] && (
        <div class={styles.stageDetail}>
          <div>
            <span class={styles.fieldLabel}>Fields added:</span>
            <span class={styles.fieldList}>
              {stages[expandedStage].fields_added?.length
                ? stages[expandedStage].fields_added!.join(", ")
                : "none"}
            </span>
          </div>
          <div>
            <span class={styles.fieldLabel}>Fields removed:</span>
            <span class={styles.fieldList}>
              {stages[expandedStage].fields_removed?.length
                ? stages[expandedStage].fields_removed!.join(", ")
                : "none"}
            </span>
          </div>
          <div>
            <span class={styles.fieldLabel}>Fields out:</span>
            <span class={styles.fieldList}>
              {stages[expandedStage].fields_out?.length
                ? stages[expandedStage].fields_out!.join(", ")
                : "all"}
            </span>
          </div>
        </div>
      )}
    </div>
  );
}

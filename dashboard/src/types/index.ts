export type Severity = "CRITICAL" | "HIGH" | "MEDIUM" | "LOW";
export type SectionKey = "overview" | "violations" | "ai" | "contracts" | "report";

export interface BlameNode {
  rank: number;
  commit_hash: string;
  author: string;
  confidence: number;
  file_path: string;
}

export interface BlastRadius {
  affected_pipelines: string[];
  affected_nodes_count: number;
  mode: "ENFORCE" | "AUDIT" | string;
}

export interface Violation {
  violation_id: string;
  severity: Severity;
  system: string;
  failing_field: string;
  check_type: string;
  records_failing: number;
  detected_at?: string;
  injected: boolean;
  message: string;
  blame_chain: BlameNode[];
  blast_radius: BlastRadius;
}

export interface ViolationsResponse {
  items: Violation[];
}

export interface HealthSummary {
  total_checks: number;
  passed: number;
  failed: number;
  contracts_monitored: number;
}

export interface HealthResponse {
  health_score: number;
  summary: HealthSummary;
  violations_by_severity: Record<string, number>;
  last_updated: string;
}

export interface EmbeddingDriftMetrics {
  score: number;
  threshold: number;
  status: "PASS" | "FAIL" | "BASELINE_SET" | string;
}

export interface PromptInputValidationMetrics {
  violation_rate: number;
  total_records: number;
  quarantined: number;
  status: "PASS" | "FAIL" | string;
}

export interface LLMOutputSchemaMetrics {
  violation_rate: number;
  trend: "RISING" | "STABLE" | "FALLING" | string;
  total_outputs_checked: number;
  status: "PASS" | "FAIL" | string;
}

export interface AIMetricsResponse {
  overall_risk: "HIGH" | "MEDIUM" | "LOW" | string;
  embedding_drift: EmbeddingDriftMetrics;
  prompt_input_validation: PromptInputValidationMetrics;
  llm_output_schema: LLMOutputSchemaMetrics;
}

export interface ContractItem {
  contract_id: string;
  owner: string;
  clause_count: number;
  last_validated?: string;
  pass_rate: number;
  yaml: string;
  human_summary: string;
  dbt_counterpart: string | null;
}

export interface ContractsResponse {
  items: ContractItem[];
}

export interface EnforcementRecommendation {
  priority: number;
  risk_level: string;
  system: string;
  action: string;
  contract_clause: string;
  file_path: string;
  estimated_impact: string;
}

export interface EnforcementReportData {
  report_id?: string;
  generated_at?: string;
  report_period?: string;
  section_1_data_health?: {
    data_health_score?: number;
    narrative?: string;
    total_checks?: number;
    total_passed?: number;
    total_failed?: number;
    critical_violations?: number;
    contracts_monitored?: number;
  };
  section_2_violations?: {
    total_violations?: number;
    by_severity?: Record<string, number>;
    top_3_violations?: Array<{
      violation_id: string;
      severity: string;
      system: string;
      field: string;
      check_type: string;
      injected: boolean;
      plain_language: string;
    }>;
  };
  section_3_schema_changes?: {
    schema_changes?: Array<{
      contract_id: string;
      system: string;
      compatibility_verdict: string;
      breaking_changes: number;
      total_changes: number;
      action_required: string;
      change_summary: string[];
    }>;
  };
  section_4_ai_risk?: {
    overall_ai_risk?: string;
    embedding_drift?: {
      status?: string;
      drift_score?: number;
      threshold?: number;
      narrative?: string;
    };
    prompt_validation?: {
      status?: string;
      violation_rate?: number;
      quarantined?: number;
      narrative?: string;
    };
    output_schema?: {
      status?: string;
      violation_rate?: number;
      trend?: string;
      narrative?: string;
    };
  };
  section_5_recommendations?: EnforcementRecommendation[];
}

export interface EnforcementReportResponse {
  generated_at: string | null;
  report_id: string | null;
  data: EnforcementReportData;
  markdown: string;
  source_json: string | null;
  source_markdown: string | null;
}

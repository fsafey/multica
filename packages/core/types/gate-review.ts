export type GateReviewOutcome = "approved" | "changes_requested";

export interface GateReviewData {
  selected_source: string;
  scope: string;
  defaults: string[];
  rights: string;
  uncertainties: string[];
  cost: string;
  changes?: string[];
  canonical_detail: Record<string, unknown>;
}

export interface GateReviewDecision {
  id: string;
  outcome: GateReviewOutcome;
  reason: string;
  actor_id: string;
  actor_name?: string;
  created_at: string;
}

export interface GateDecisionWake {
  state: "pending" | "delivered";
  task_id?: string;
}

export interface GateReviewRequest {
  id: string;
  issue_id: string;
  gate: string;
  revision: number;
  subject_digest: string;
  review: GateReviewData;
  actor_type: "member" | "agent";
  actor_id: string;
  created_at: string;
  decision?: GateReviewDecision;
  wake?: GateDecisionWake;
}

export interface GateReviewsResponse {
  gate_reviews: GateReviewRequest[];
}

export interface CreateGateReviewDecisionResponse {
  request: GateReviewRequest;
  decision: GateReviewDecision;
  wake: GateDecisionWake;
}

import { queryOptions, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";
import { issueKeys } from "./queries";
import type { GateReviewOutcome } from "../types/gate-review";

export function gateReviewsOptions(issueId: string) {
  return queryOptions({
    queryKey: issueKeys.gateReviews(issueId),
    queryFn: () => api.listGateReviews(issueId),
  });
}

export function useCreateGateReviewDecision(issueId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      requestId,
      outcome,
      reason,
    }: {
      requestId: string;
      outcome: GateReviewOutcome;
      reason?: string;
    }) => api.createGateReviewDecision(issueId, requestId, outcome, reason),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: issueKeys.gateReviews(issueId) });
      queryClient.invalidateQueries({ queryKey: issueKeys.tasks(issueId) });
      queryClient.invalidateQueries({ queryKey: issueKeys.timeline(issueId) });
    },
  });
}

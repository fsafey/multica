"use client";

import { useCallback, useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Check, ClipboardCheck, RotateCcw } from "lucide-react";
import { gateReviewsOptions, useCreateGateReviewDecision } from "@multica/core/issues";
import type { GateReviewRequest } from "@multica/core/types";
import { Button } from "@multica/ui/components/ui/button";
import { Textarea } from "@multica/ui/components/ui/textarea";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@multica/ui/components/ui/alert-dialog";
import { cn } from "@multica/ui/lib/utils";
import { toast } from "sonner";
import { useWSEvent, useWSReconnect } from "@multica/core/realtime";
import { useT } from "../../i18n";

function ReviewList({ values, empty }: { values: string[]; empty: string }) {
  if (values.length === 0) return <span className="text-muted-foreground">{empty}</span>;
  return (
    <ul className="space-y-1">
      {values.map((value) => <li key={value}>{value}</li>)}
    </ul>
  );
}

function GateReviewCard({ issueId, review }: { issueId: string; review: GateReviewRequest }) {
  const { t } = useT("issues");
  const [requestingChanges, setRequestingChanges] = useState(false);
  const [approveOpen, setApproveOpen] = useState(false);
  const [reason, setReason] = useState("");
  const decision = useCreateGateReviewDecision(issueId);
  const decided = review.decision !== undefined;
  const stateLabel = review.decision?.outcome === "approved"
    ? t(($) => $.gate_review.approved)
    : review.decision?.outcome === "changes_requested"
      ? t(($) => $.gate_review.changes_requested)
      : t(($) => $.gate_review.awaiting_decision);
  const actor = review.decision?.actor_name || review.decision?.actor_id || review.actor_name || review.actor_id;
  const actorType = review.actor_type === "agent"
    ? t(($) => $.gate_review.actor_agent)
    : t(($) => $.gate_review.actor_member);
  const eventTime = review.decision?.created_at || review.created_at;
  const eventLabel = review.decision
    ? t(($) => $.gate_review.decided_event, { state: stateLabel, actor })
    : t(($) => $.gate_review.requested_event, { actorType, actor, state: stateLabel });
  const wakeState = review.wake?.state === "delivered"
    ? t(($) => $.gate_review.wake_delivered)
    : t(($) => $.gate_review.wake_pending);

  const submit = async (outcome: "approved" | "changes_requested") => {
    try {
      await decision.mutateAsync({
        requestId: review.id,
        outcome,
        ...(outcome === "changes_requested" && reason.trim() ? { reason: reason.trim() } : {}),
      });
      setApproveOpen(false);
      setRequestingChanges(false);
      setReason("");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : t(($) => $.gate_review.save_error));
    }
  };

  return (
    <section className="rounded-xl border bg-card p-4 shadow-sm" aria-labelledby={`gate-review-${review.id}`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex items-center gap-2">
            <ClipboardCheck className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
            <h3 id={`gate-review-${review.id}`} className="font-semibold">
              {t(($) => $.gate_review.title, { gate: review.gate, revision: review.revision })}
            </h3>
          </div>
          <p className="mt-1 text-xs text-muted-foreground">
            {eventLabel} {t(($) => $.gate_review.at)} <time dateTime={eventTime}>{new Date(eventTime).toLocaleString()}</time>
          </p>
        </div>
        {review.wake && (
          <span className={cn(
            "rounded-full px-2 py-1 text-xs",
            review.wake.state === "delivered" ? "bg-primary/10 text-primary" : "bg-muted text-muted-foreground",
          )}>
            {t(($) => $.gate_review.wake, { state: wakeState })}
          </span>
        )}
      </div>

      <dl className="mt-4 grid gap-3 text-sm sm:grid-cols-2">
        <div><dt className="font-medium text-muted-foreground">{t(($) => $.gate_review.selected_source)}</dt><dd className="mt-1">{review.review.selected_source}</dd></div>
        <div><dt className="font-medium text-muted-foreground">{t(($) => $.gate_review.scope)}</dt><dd className="mt-1">{review.review.scope}</dd></div>
        <div><dt className="font-medium text-muted-foreground">{t(($) => $.gate_review.defaults)}</dt><dd className="mt-1"><ReviewList values={review.review.defaults} empty={t(($) => $.gate_review.no_defaults)} /></dd></div>
        <div><dt className="font-medium text-muted-foreground">{t(($) => $.gate_review.rights)}</dt><dd className="mt-1">{review.review.rights}</dd></div>
        <div><dt className="font-medium text-muted-foreground">{t(($) => $.gate_review.uncertainties)}</dt><dd className="mt-1"><ReviewList values={review.review.uncertainties} empty={t(($) => $.gate_review.none_recorded)} /></dd></div>
        <div><dt className="font-medium text-muted-foreground">{t(($) => $.gate_review.cost)}</dt><dd className="mt-1">{review.review.cost}</dd></div>
        {review.review.changes && review.review.changes.length > 0 && (
          <div className="sm:col-span-2"><dt className="font-medium text-muted-foreground">{t(($) => $.gate_review.changes)}</dt><dd className="mt-1"><ReviewList values={review.review.changes} empty={t(($) => $.gate_review.no_changes)} /></dd></div>
        )}
        {review.decision?.outcome === "changes_requested" && review.decision.reason && (
          <div className="sm:col-span-2"><dt className="font-medium text-muted-foreground">{t(($) => $.gate_review.reason_label)}</dt><dd className="mt-1 whitespace-pre-wrap">{review.decision.reason}</dd></div>
        )}
      </dl>

      <details className="mt-4 text-xs text-muted-foreground">
        <summary className="cursor-pointer font-medium">{t(($) => $.gate_review.canonical_subject)}</summary>
        <div className="mt-2 break-all font-mono">{review.subject_digest}</div>
        <pre className="mt-2 max-h-64 overflow-auto rounded-md bg-muted p-3 text-foreground">{JSON.stringify(review.review.canonical_detail, null, 2)}</pre>
      </details>

      {!decided && (
        <div className="mt-4 border-t pt-4">
          {requestingChanges && (
            <div className="mb-3 space-y-2">
              <label className="text-sm font-medium" htmlFor={`gate-change-reason-${review.id}`}>{t(($) => $.gate_review.reason_label)}</label>
              <Textarea
                id={`gate-change-reason-${review.id}`}
                value={reason}
                onChange={(event) => setReason(event.target.value)}
                maxLength={4000}
              />
            </div>
          )}
          <div className="flex flex-wrap gap-2">
            <Button
              size="sm"
              onClick={() => setApproveOpen(true)}
              disabled={decision.isPending}
              aria-label={t(($) => $.gate_review.approve_revision, { revision: review.revision })}
            >
              <Check className="h-4 w-4" /> {t(($) => $.gate_review.approve_revision, { revision: review.revision })}
            </Button>
            {!requestingChanges ? (
              <Button size="sm" variant="outline" onClick={() => setRequestingChanges(true)} disabled={decision.isPending}>
                <RotateCcw className="h-4 w-4" /> {t(($) => $.gate_review.request_changes)}
              </Button>
            ) : (
              <>
                <Button size="sm" variant="destructive" onClick={() => void submit("changes_requested")} disabled={decision.isPending}>
                  {t(($) => $.gate_review.submit_changes)}
                </Button>
                <Button size="sm" variant="ghost" onClick={() => setRequestingChanges(false)} disabled={decision.isPending}>{t(($) => $.gate_review.cancel)}</Button>
              </>
            )}
          </div>
        </div>
      )}

      <AlertDialog open={approveOpen} onOpenChange={setApproveOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t(($) => $.gate_review.dialog_title, { gate: review.gate, revision: review.revision })}</AlertDialogTitle>
            <AlertDialogDescription>
              {t(($) => $.gate_review.dialog_description)}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t(($) => $.gate_review.cancel)}</AlertDialogCancel>
            <AlertDialogAction onClick={() => void submit("approved")} disabled={decision.isPending}>
              {t(($) => $.gate_review.confirm_approval)}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </section>
  );
}

export function GateReviewPanel({ issueId }: { issueId: string }) {
  const queryClient = useQueryClient();
  const { data } = useQuery(gateReviewsOptions(issueId));
  const refresh = useCallback(() => {
    queryClient.invalidateQueries({ queryKey: gateReviewsOptions(issueId).queryKey });
  }, [issueId, queryClient]);
  useWSEvent("comment:created", useCallback((payload: unknown) => {
    const commentIssueID = (payload as { comment?: { issue_id?: string } }).comment?.issue_id;
    if (commentIssueID === issueId) refresh();
  }, [issueId, refresh]));
  useWSReconnect(refresh);
  const current = useMemo(() => {
    const seen = new Set<string>();
    return [...(data?.gate_reviews ?? [])]
      .sort((a, b) => b.revision - a.revision || b.created_at.localeCompare(a.created_at))
      .filter((review) => {
      if (seen.has(review.gate)) return false;
      seen.add(review.gate);
      return true;
      });
  }, [data]);

  if (current.length === 0) return null;
  return <div className="mt-6 space-y-3">{current.map((review) => <GateReviewCard key={review.id} issueId={issueId} review={review} />)}</div>;
}

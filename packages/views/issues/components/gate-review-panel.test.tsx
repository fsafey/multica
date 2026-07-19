// @vitest-environment jsdom

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { GateReviewRequest } from "@multica/core/types";
import { I18nProvider } from "@multica/core/i18n/react";
import enIssues from "../../locales/en/issues.json";

const mockState = vi.hoisted(() => ({
  reviews: [] as GateReviewRequest[],
  mutateAsync: vi.fn(),
}));

vi.mock("@multica/core/issues", () => ({
  gateReviewsOptions: () => ({
    queryKey: ["issues", "gate-reviews", "issue-1"],
    queryFn: async () => ({ gate_reviews: mockState.reviews }),
  }),
  useCreateGateReviewDecision: () => ({
    mutateAsync: mockState.mutateAsync,
    isPending: false,
  }),
}));

vi.mock("@multica/core/realtime", () => ({
  useWSEvent: vi.fn(),
  useWSReconnect: vi.fn(),
}));

import { GateReviewPanel } from "./gate-review-panel";

function request(overrides: Partial<GateReviewRequest> = {}): GateReviewRequest {
  return {
    id: "review-1",
    issue_id: "issue-1",
    gate: "P0",
    revision: 3,
    subject_digest: `sha256:${"a".repeat(64)}`,
    review: {
      selected_source: "Attachment att-123",
      scope: "Translate one supplied book",
      defaults: ["Standard publication profile"],
      rights: "Scholar supplied source",
      uncertainties: ["Page extent remains unknown"],
      cost: "No paid extraction authorized",
      changes: ["Selected attachment changed"],
      canonical_detail: { source: { attachment_id: "att-123" }, intake_revision: 3 },
    },
    actor_type: "agent",
    actor_id: "agent-1",
    actor_name: "Pub Intake",
    created_at: "2026-07-19T12:00:00Z",
    ...overrides,
  };
}

function renderPanel() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <I18nProvider locale="en" resources={{ en: { issues: enIssues } }}>
      <QueryClientProvider client={client}>
        <GateReviewPanel issueId="issue-1" />
      </QueryClientProvider>
    </I18nProvider>,
  );
}

beforeEach(() => {
  mockState.reviews = [request()];
  mockState.mutateAsync.mockReset().mockResolvedValue({});
});

describe("GateReviewPanel", () => {
  it("shows the complete deterministic review sheet without control-token syntax", async () => {
    renderPanel();
    expect(await screen.findByText("Attachment att-123")).toBeInTheDocument();
    expect(screen.getByText("Translate one supplied book")).toBeInTheDocument();
    expect(screen.getByText("Standard publication profile")).toBeInTheDocument();
    expect(screen.getByText("Scholar supplied source")).toBeInTheDocument();
    expect(screen.getByText("Page extent remains unknown")).toBeInTheDocument();
    expect(screen.getByText("No paid extraction authorized")).toBeInTheDocument();
    expect(screen.getByText("Selected attachment changed")).toBeInTheDocument();
    expect(screen.getByText(/Requested by agent Pub Intake/)).toBeInTheDocument();
    fireEvent.click(screen.getByText("Canonical decision subject"));
    expect(screen.getByText(/"attachment_id": "att-123"/)).toBeInTheDocument();
    expect(screen.queryByText(/GATE APPROVED|GATE REJECTED|MANIFEST ACCEPTED/)).not.toBeInTheDocument();
  });

  it("has revision-bound accessible actions and an optional reason", async () => {
    renderPanel();
    const approve = await screen.findByRole("button", { name: "Approve revision 3" });
    fireEvent.click(approve);
    expect(screen.getByRole("alertdialog")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Confirm approval" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Confirm approval" }));
    await waitFor(() => expect(screen.queryByRole("alertdialog")).not.toBeInTheDocument());
    expect(mockState.mutateAsync).toHaveBeenCalledWith({ requestId: "review-1", outcome: "approved" });
    fireEvent.click(screen.getByRole("button", { name: "Request changes" }));
    expect(screen.getByLabelText("Reason for requested changes (optional)")).toBeInTheDocument();
  });

  it("shows the immutable member decision and hides actions after decision", async () => {
    mockState.reviews = [request({
      decision: {
        id: "decision-1",
        outcome: "approved",
        reason: "",
        actor_id: "member-1",
        actor_name: "Faried",
        created_at: "2026-07-19T12:05:00Z",
      },
      wake: { state: "delivered", task_id: "task-1" },
    })];
    renderPanel();
    expect(await screen.findByText(/Approved by member Faried/)).toBeInTheDocument();
    expect(screen.getByText("Assignee wake delivered")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Approve revision/ })).not.toBeInTheDocument();
  });

  it("shows the immutable requested-changes reason", async () => {
    mockState.reviews = [request({
      decision: {
        id: "decision-1",
        outcome: "changes_requested",
        reason: "Use the corrected book-law logical name.",
        actor_id: "member-1",
        actor_name: "Faried",
        created_at: "2026-07-19T12:05:00Z",
      },
      wake: { state: "pending" },
    })];
    renderPanel();
    expect(await screen.findByText(/Changes requested by member Faried/)).toBeInTheDocument();
    expect(screen.getByText("Use the corrected book-law logical name.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Approve revision/ })).not.toBeInTheDocument();
  });

  it("selects the highest revision per gate even when the API response is unordered", async () => {
    mockState.reviews = [request({ id: "older", revision: 2 }), request({ id: "newer", revision: 4 })];
    renderPanel();
    expect(await screen.findByText("Gate P0 - revision 4")).toBeInTheDocument();
    expect(screen.queryByText("Gate P0 - revision 2")).not.toBeInTheDocument();
  });
});

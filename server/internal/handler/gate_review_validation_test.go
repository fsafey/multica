package handler

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func validGateReviewRequest() CreateGateReviewRequestBody {
	return CreateGateReviewRequestBody{
		Gate:          "P0",
		Revision:      2,
		SubjectDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Review: GateReviewData{
			SelectedSource:  "Attachment att-123",
			Scope:           "Translate one supplied book",
			Defaults:        []string{"Use the standard publication profile"},
			Rights:          "Scholar supplied source for this publication",
			Uncertainties:   []string{"Page extent remains unknown"},
			Cost:            "No paid extraction authorized",
			Changes:         []string{"Selected attachment changed"},
			CanonicalDetail: json.RawMessage(`{"source":{"attachment_id":"att-123"}}`),
		},
	}
}

func TestGateReviewTimestampPreservesSameSecondOrdering(t *testing.T) {
	t.Parallel()
	request := gateReviewTimestamp(pgtype.Timestamptz{
		Time:  time.Date(2026, 7, 19, 12, 30, 45, 123456000, time.FixedZone("offset", -4*60*60)),
		Valid: true,
	})
	decision := gateReviewTimestamp(pgtype.Timestamptz{
		Time:  time.Date(2026, 7, 19, 12, 30, 45, 123457000, time.FixedZone("offset", -4*60*60)),
		Valid: true,
	})
	if request != "2026-07-19T16:30:45.123456Z" {
		t.Fatalf("request timestamp = %q", request)
	}
	if decision != "2026-07-19T16:30:45.123457Z" {
		t.Fatalf("decision timestamp = %q", decision)
	}
	if request >= decision {
		t.Fatalf("same-second ordering lost: request=%s decision=%s", request, decision)
	}
}

func TestGateReviewJSONEqualIgnoresJSONBKeyOrder(t *testing.T) {
	t.Parallel()
	left := []byte(`{"scope":"book","canonical_detail":{"revision":2,"source":"att"}}`)
	right := []byte(`{"canonical_detail":{"source":"att","revision":2},"scope":"book"}`)
	if !gateReviewJSONEqual(left, right) {
		t.Fatal("semantically identical JSON was not idempotent")
	}
}

func TestValidateCreateGateReviewRequest(t *testing.T) {
	t.Parallel()
	if err := validateCreateGateReviewRequest(validGateReviewRequest()); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*CreateGateReviewRequestBody)
	}{
		{name: "zero revision", mutate: func(r *CreateGateReviewRequestBody) { r.Revision = 0 }},
		{name: "bare digest", mutate: func(r *CreateGateReviewRequestBody) { r.SubjectDigest = r.SubjectDigest[7:] }},
		{name: "uppercase digest", mutate: func(r *CreateGateReviewRequestBody) {
			r.SubjectDigest = "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		}},
		{name: "missing source", mutate: func(r *CreateGateReviewRequestBody) { r.Review.SelectedSource = "" }},
		{name: "missing cost", mutate: func(r *CreateGateReviewRequestBody) { r.Review.Cost = "" }},
		{name: "missing defaults", mutate: func(r *CreateGateReviewRequestBody) { r.Review.Defaults = nil }},
		{name: "missing uncertainties", mutate: func(r *CreateGateReviewRequestBody) { r.Review.Uncertainties = nil }},
		{name: "missing canonical detail", mutate: func(r *CreateGateReviewRequestBody) { r.Review.CanonicalDetail = nil }},
		{name: "canonical detail is not an object", mutate: func(r *CreateGateReviewRequestBody) {
			r.Review.CanonicalDetail = json.RawMessage(`[]`)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := validGateReviewRequest()
			tt.mutate(&r)
			if err := validateCreateGateReviewRequest(r); err == nil {
				t.Fatal("invalid request accepted")
			}
		})
	}
}

package handler

import "testing"

func TestContainsReservedHumanGateControl(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{name: "approval", content: "[GATE APPROVED] pass P0", want: true},
		{name: "rejection", content: "[GATE REJECTED] pass I2\nNeeds work", want: true},
		{name: "manifest", content: "[MANIFEST ACCEPTED]", want: true},
		{name: "escaped", content: `\[GATE APPROVED\] pass P0`, want: true},
		{name: "quoted example", content: "> `[GATE REJECTED] pass P0`", want: true},
		{name: "review marker remains agent writable", content: "[GATE REVIEW] pass P0", want: false},
		{name: "ordinary prose", content: "The member decision is ready for review.", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := containsReservedHumanGateControl(tt.content); got != tt.want {
				t.Fatalf("containsReservedHumanGateControl() = %v, want %v", got, tt.want)
			}
		})
	}
}

import { describe, expect, it } from "vitest";
import { decideExternalOwner, ownershipRecordIsStale } from "./daemon-ownership";

const record = { pid: 42, health_port: 19522, profile: "desktop-other" };
const healthy = {
  status: "running",
  os: "darwin",
  server_url: "http://localhost:8080",
  cli_version: "v0.4.9",
};

describe("decideExternalOwner", () => {

  it("treats an unlocked stale owner record as recoverable", () => {
    expect(
      ownershipRecordIsStale({ pid: 999_999, health_port: 19514, profile: "desktop-old" }, () => false),
    ).toBe(true);
    expect(
      ownershipRecordIsStale({ pid: 42, health_port: 19514, profile: "desktop-live" }, () => true),
    ).toBe(false);
    expect(ownershipRecordIsStale({ health_port: 19514, profile: "unknown" }, () => false)).toBe(false);
    expect(
      ownershipRecordIsStale(
        { pid: 42, started_at: "2026-01-01T00:00:00Z" },
        () => true,
        Date.parse("2026-02-01T00:00:00Z"),
      ),
    ).toBe(true);
  });

  it("adopts a compatible owner from another profile", () => {
    expect(
      decideExternalOwner(record, healthy, "http://localhost:8080", "v0.4.9", "darwin", true),
    ).toEqual({ kind: "adopt", profile: "desktop-other" });
  });

  it("treats an omitted profile as the default profile", () => {
    expect(
      decideExternalOwner(
        { pid: 42, health_port: 19514 },
        healthy,
        "http://localhost:8080",
        "v0.4.9",
        "darwin",
        true,
      ),
    ).toEqual({ kind: "adopt", profile: "" });
  });

  it.each([
    ["invalid record", { pid: 42, health_port: "bad", profile: "desktop-other" }, healthy],
    ["stale owner", record, null],
    ["foreign owner", record, { ...healthy, os: "linux" }],
    ["server mismatch", record, { ...healthy, server_url: "http://other:8080" }],
    ["CLI mismatch", record, { ...healthy, cli_version: "v0.4.8" }],
  ])("blocks %s rather than allowing Desktop Start", (_name, candidate, health) => {
    const decision = decideExternalOwner(
      candidate,
      health,
      "http://localhost:8080",
      "v0.4.9",
      "darwin",
      true,
    );
    expect(decision.kind).toBe("block");
    if (decision.kind === "block") expect(decision.reason).toMatch(/Start|owner/i);
  });

  it.each([
    ["unknown server", null, "v0.4.9", true],
    ["unknown bundled CLI", "http://localhost:8080", null, true],
    ["foreign account", "http://localhost:8080", "v0.4.9", false],
  ])("blocks %s", (_name, server, cliVersion, identityMatches) => {
    expect(
      decideExternalOwner(
        record,
        healthy,
        server,
        cliVersion,
        "darwin",
        identityMatches,
      ).kind,
    ).toBe("block");
  });
});

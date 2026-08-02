// Decision logic for an ownership record found outside Desktop's dedicated
// profile. Kept pure so the main process can fail closed without making the
// Electron lifecycle state machine itself depend on filesystem or HTTP mocks.

export interface OwnershipRecord {
  pid?: unknown;
  health_port?: unknown;
  profile?: unknown;
  version?: unknown;
  started_at?: unknown;
}

// A daemon lock is advisory. When its recorded PID is known to be gone, the
// operating system has already released that advisory lock, so its JSON body is
// stale diagnostics only. Desktop must allow the normal CLI start path to
// recover without manual deletion of daemon.lock. Unknown or still-live PIDs
// remain fail-closed.
export function ownershipRecordIsStale(
  record: OwnershipRecord,
  processExists: (pid: number) => boolean,
  bootStartedAtMs = Number.NEGATIVE_INFINITY,
): boolean {
  if (typeof record.started_at === "string") {
    const ownerStartedAtMs = Date.parse(record.started_at);
    if (
      Number.isFinite(ownerStartedAtMs) &&
      ownerStartedAtMs < bootStartedAtMs
    ) {
      return true;
    }
  }
  return (
    Number.isInteger(record.pid) &&
    (record.pid as number) > 0 &&
    !processExists(record.pid as number)
  );
}

export interface OwnershipHealth {
  status?: string;
  os?: string;
  server_url?: string;
  cli_version?: string;
}

export type ExternalOwnerDecision =
  | { kind: "adopt"; profile: string }
  | { kind: "block"; reason: string };

function ownerProfile(record: OwnershipRecord): string | null {
  if (record.profile === undefined) return "";
  return typeof record.profile === "string" ? record.profile : null;
}

function normalizeServerURL(value: string): string {
  return value.replace(/\/+$/, "").toLowerCase();
}

function stopHint(profile: string): string {
  return profile ? `multica daemon stop --profile ${profile}` : "multica daemon stop";
}

export function decideExternalOwner(
  record: OwnershipRecord,
  health: OwnershipHealth | null,
  expectedServerURL: string | null,
  bundledCLIVersion: string | null,
  hostOS: string,
  identityMatches: boolean,
): ExternalOwnerDecision {
  const profile = ownerProfile(record);
  if (
    !Number.isInteger(record.pid) ||
    (record.pid as number) <= 0 ||
    !Number.isInteger(record.health_port) ||
    (record.health_port as number) <= 0 ||
    profile === null
  ) {
    return {
      kind: "block",
      reason:
        "The daemon ownership record is invalid. Start is disabled until the recorded owner is inspected.",
    };
  }
  if (!expectedServerURL) {
    return {
      kind: "block",
      reason:
        "Desktop cannot verify which server the daemon owner belongs to. Start is disabled until account setup completes.",
    };
  }
  if (!bundledCLIVersion) {
    return {
      kind: "block",
      reason:
        "Desktop cannot verify the daemon owner's CLI compatibility. Start is disabled until the bundled CLI is available.",
    };
  }
  if (!health || health.status !== "running") {
    return {
      kind: "block",
      reason: `The daemon owner for profile ${profile || "default"} is not healthy. Start is disabled until it releases ownership. Inspect it or run ${stopHint(profile)}.`,
    };
  }
  if (health.os !== hostOS) {
    return {
      kind: "block",
      reason: `The daemon owner for profile ${profile || "default"} runs on ${health.os || "an unknown OS"}, not this Desktop host. Manage it from that environment with ${stopHint(profile)}.`,
    };
  }
  if (
    !health.server_url ||
    normalizeServerURL(health.server_url) !== normalizeServerURL(expectedServerURL)
  ) {
    return {
      kind: "block",
      reason: `The daemon owner for profile ${profile || "default"} is connected to a different server. Start is disabled to avoid a second owner. Stop it with ${stopHint(profile)} before retrying.`,
    };
  }
  if (
    !health.cli_version ||
    health.cli_version !== bundledCLIVersion
  ) {
    return {
      kind: "block",
      reason: `The daemon owner for profile ${profile || "default"} uses CLI ${health.cli_version || "unknown"}, not bundled CLI ${bundledCLIVersion}. Update it or stop it with ${stopHint(profile)} before starting Desktop.`,
    };
  }
  if (!identityMatches) {
    return {
      kind: "block",
      reason: `The daemon owner for profile ${profile || "default"} belongs to a different or unverifiable account. Stop it with ${stopHint(profile)} before starting Desktop.`,
    };
  }
  return { kind: "adopt", profile };
}

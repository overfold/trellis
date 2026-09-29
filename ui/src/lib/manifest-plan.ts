export interface ManifestChange {
  operation: "add" | "remove" | "change";
  path: string;
  before?: unknown;
  after?: unknown;
}

// ManifestPlan is returned by the Trellis control plane. The dashboard only
// renders this contract; it does not decide what a manifest change means.
export interface ManifestPlan {
  action: "create" | "update" | "none";
  namespace: string;
  job: string;
  // base_version is the job version this plan was computed against. Applying
  // the plan sends it as expected_version so a concurrent change is rejected.
  base_version?: number;
  base_revision?: number;
  desired_allocations: number;
  changes: ManifestChange[];
}

export function planTitle(plan: ManifestPlan): string {
  switch (plan.action) {
    case "create":
      return `Create ${plan.namespace}/${plan.job} (${plan.desired_allocations} desired allocation${plan.desired_allocations === 1 ? "" : "s"})`;
    case "none":
      return `No changes to ${plan.namespace}/${plan.job} (version ${plan.base_version ?? 0}, revision ${plan.base_revision ?? 0})`;
    case "update":
      return `Update ${plan.namespace}/${plan.job} from version ${plan.base_version ?? 0} (revision ${plan.base_revision ?? 0})`;
  }
}

// expectedVersionForPlan returns the expected_version an apply of plan must
// send: 0 requires that the job still does not exist.
export function expectedVersionForPlan(plan: ManifestPlan): number {
  return plan.action === "create" ? 0 : plan.base_version ?? 0;
}

export function formatPlanValue(path: string, value: unknown): string {
  if (value === null || value === undefined) return "null";
  if (typeof value === "number" && isDurationPath(path)) {
    return formatNanoseconds(value);
  }
  if (typeof value === "number" && isMemoryPath(path)) {
    return formatBytes(value);
  }
  if (typeof value === "string") return JSON.stringify(value);
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

function isDurationPath(path: string) {
  return (
    path.endsWith(".window") ||
    path.endsWith(".interval") ||
    path.endsWith(".timeout")
  );
}

function isMemoryPath(path: string) {
  return path.endsWith(".resources.memory");
}

function formatNanoseconds(value: number): string {
  const units: Array<[number, string]> = [
    [3_600_000_000_000, "h"],
    [60_000_000_000, "m"],
    [1_000_000_000, "s"],
    [1_000_000, "ms"],
    [1_000, "µs"],
    [1, "ns"],
  ];
  if (value === 0) return "0s";
  let remaining = Math.abs(value);
  let result = "";
  for (const [size, suffix] of units) {
    if (remaining < size) continue;
    const amount = Math.floor(remaining / size);
    remaining -= amount * size;
    result += `${amount}${suffix}`;
  }
  return value < 0 ? `-${result}` : result;
}

function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value < 0) return String(value);
  if (value === 0) return "0B";
  const units: Array<[number, string]> = [
    [2 ** 40, "TiB"],
    [2 ** 30, "GiB"],
    [2 ** 20, "MiB"],
    [2 ** 10, "KiB"],
  ];
  for (const [size, suffix] of units) {
    if (value >= size && value % size === 0) {
      return `${value / size}${suffix}`;
    }
  }
  return `${value}B`;
}

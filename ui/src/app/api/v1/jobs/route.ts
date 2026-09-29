import { NextRequest, NextResponse } from "next/server";
import {
  orchestratorHeaders,
  TRELLIS_URL,
  getAllowWrites,
  resolveDashboardNamespace,
} from "@/lib/orchestrator";

export async function GET(request: NextRequest) {
  const selected = resolveDashboardNamespace(request);
  if (selected.error) {
    return NextResponse.json({ error: selected.error }, { status: 403 });
  }
  try {
    const res = await fetch(`${TRELLIS_URL}/v1/jobs`, {
      headers: orchestratorHeaders(selected.namespace),
    });

    if (!res.ok) {
      return NextResponse.json(
        { error: `Upstream error: ${res.status}` },
        { status: res.status },
      );
    }

    const data = await res.json();
    return NextResponse.json(data);
  } catch {
    return NextResponse.json(
      { error: "Failed to connect to orchestrator" },
      { status: 502 },
    );
  }
}

export async function POST(request: NextRequest) {
  if (!getAllowWrites()) {
    return NextResponse.json(
      { error: "Dashboard is read-only" },
      { status: 403 },
    );
  }
  const selected = resolveDashboardNamespace(request);
  if (selected.error) {
    return NextResponse.json({ error: selected.error }, { status: 403 });
  }
  try {
    const body = await request.json();
    if (!body?.spec || typeof body.spec !== "object") {
      return NextResponse.json({ error: "Missing job spec" }, { status: 400 });
    }
    if (body.spec.namespace !== selected.namespace) {
      return NextResponse.json(
        {
          error: `Manifest namespace ${JSON.stringify(body.spec.namespace)} does not match active namespace ${JSON.stringify(selected.namespace)}. Change the manifest or select the intended namespace.`,
        },
        { status: 422 },
      );
    }

    const expectedVersion: unknown = body.expected_version;
    if (
      expectedVersion !== undefined &&
      (typeof expectedVersion !== "number" ||
        !Number.isInteger(expectedVersion) ||
        expectedVersion < 0)
    ) {
      return NextResponse.json(
        { error: "expected_version must be a non-negative integer" },
        { status: 400 },
      );
    }

    const res = await fetch(`${TRELLIS_URL}/v1/jobs`, {
      method: "POST",
      headers: {
        ...orchestratorHeaders(selected.namespace),
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ spec: body.spec, expected_version: expectedVersion }),
    });

    const text = await res.text();
    let data: unknown = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = null;
    }
    if (!res.ok) {
      if (typeof data === "object" && data !== null) {
        const record = data as Record<string, unknown>;
        // The control plane reports plain errors, including 409 version
        // conflicts, as {"message": ...}; validation failures carry issues.
        if (typeof record.message === "string" && record.error === undefined) {
          return NextResponse.json(
            { error: record.message },
            { status: res.status },
          );
        }
        return NextResponse.json(data, { status: res.status });
      }
      return NextResponse.json(
        { error: text.trim() || `Upstream error: ${res.status}` },
        { status: res.status },
      );
    }

    return NextResponse.json(data, { status: 202 });
  } catch {
    return NextResponse.json(
      { error: "Failed to connect to orchestrator" },
      { status: 502 },
    );
  }
}

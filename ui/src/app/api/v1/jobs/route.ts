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
      { error: "Console is read-only" },
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

    const res = await fetch(`${TRELLIS_URL}/v1/jobs`, {
      method: "POST",
      headers: {
        ...orchestratorHeaders(selected.namespace),
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const text = await res.text();
      return NextResponse.json(
        { error: text || `Upstream error: ${res.status}` },
        { status: res.status },
      );
    }

    return new NextResponse(null, { status: 202 });
  } catch {
    return NextResponse.json(
      { error: "Failed to connect to orchestrator" },
      { status: 502 },
    );
  }
}

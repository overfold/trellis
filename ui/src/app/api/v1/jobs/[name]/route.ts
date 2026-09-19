import { NextResponse } from "next/server";
import {
  orchestratorHeaders,
  TRELLIS_URL,
  getAllowWrites,
  resolveDashboardNamespace,
} from "@/lib/orchestrator";

export async function GET(
  request: Request,
  { params }: { params: Promise<{ name: string }> }
) {
  const selected = resolveDashboardNamespace(request);
  if (selected.error) {
    return NextResponse.json({ error: selected.error }, { status: 403 });
  }
  const { name } = await params;
  try {
    const res = await fetch(
      `${TRELLIS_URL}/v1/jobs/${encodeURIComponent(name)}`,
      {
        headers: orchestratorHeaders(selected.namespace),
      }
    );

    if (!res.ok) {
      return NextResponse.json(
        { error: `Upstream error: ${res.status}` },
        { status: res.status }
      );
    }

    const data = await res.json();
    return NextResponse.json(data);
  } catch {
    return NextResponse.json(
      { error: "Failed to connect to orchestrator" },
      { status: 502 }
    );
  }
}

export async function DELETE(
  request: Request,
  { params }: { params: Promise<{ name: string }> }
) {
  if (!getAllowWrites()) {
    return NextResponse.json({ error: "Console is read-only" }, { status: 403 });
  }
  const selected = resolveDashboardNamespace(request);
  if (selected.error) {
    return NextResponse.json({ error: selected.error }, { status: 403 });
  }
  const { name } = await params;
  try {
    const res = await fetch(
      `${TRELLIS_URL}/v1/jobs/${encodeURIComponent(name)}`,
      {
        method: "DELETE",
        headers: orchestratorHeaders(selected.namespace),
      }
    );

    if (!res.ok) {
      return NextResponse.json(
        { error: `Upstream error: ${res.status}` },
        { status: res.status }
      );
    }

    return new NextResponse(null, { status: 204 });
  } catch {
    return NextResponse.json(
      { error: "Failed to connect to orchestrator" },
      { status: 502 }
    );
  }
}

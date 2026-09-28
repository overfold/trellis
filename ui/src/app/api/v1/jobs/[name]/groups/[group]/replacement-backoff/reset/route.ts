import { NextResponse } from "next/server";
import {
  orchestratorHeaders,
  TRELLIS_URL,
  getAllowWrites,
  resolveDashboardNamespace,
} from "@/lib/orchestrator";

export async function POST(
  request: Request,
  { params }: { params: Promise<{ name: string; group: string }> },
) {
  if (!getAllowWrites()) {
    return NextResponse.json({ error: "Dashboard is read-only" }, { status: 403 });
  }
  const selected = resolveDashboardNamespace(request);
  if (selected.error) {
    return NextResponse.json({ error: selected.error }, { status: 403 });
  }
  const { name, group } = await params;
  try {
    const res = await fetch(
      `${TRELLIS_URL}/v1/jobs/${encodeURIComponent(name)}/groups/${encodeURIComponent(group)}/replacement-backoff/reset`,
      {
        method: "POST",
        headers: orchestratorHeaders(selected.namespace),
      },
    );

    if (!res.ok) {
      return NextResponse.json(
        { error: `Upstream error: ${res.status}` },
        { status: res.status },
      );
    }

    return new NextResponse(null, { status: 204 });
  } catch {
    return NextResponse.json(
      { error: "Failed to connect to orchestrator" },
      { status: 502 },
    );
  }
}

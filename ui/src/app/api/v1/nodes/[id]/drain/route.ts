import { NextResponse } from "next/server";
import {
  orchestratorHeaders,
  TRELLIS_URL,
  getAllowWrites,
} from "@/lib/orchestrator";

export async function POST(
  _request: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  if (!getAllowWrites()) {
    return NextResponse.json(
      { error: "Console is read-only" },
      { status: 403 },
    );
  }
  const { id } = await params;
  try {
    const res = await fetch(
      `${TRELLIS_URL}/v1/nodes/${encodeURIComponent(id)}/drain`,
      { method: "POST", headers: orchestratorHeaders() },
    );
    if (!res.ok) {
      return NextResponse.json(
        { error: `Upstream error: ${res.status}` },
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

export async function DELETE(
  _request: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  if (!getAllowWrites()) {
    return NextResponse.json(
      { error: "Console is read-only" },
      { status: 403 },
    );
  }
  const { id } = await params;
  try {
    const res = await fetch(
      `${TRELLIS_URL}/v1/nodes/${encodeURIComponent(id)}/drain`,
      { method: "DELETE", headers: orchestratorHeaders() },
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

import { parseTraceparent } from "@space/contracts/observability";
import type { Headers, Sinks } from "@temporalio/workflow";

export const traceHeader = "space-traceparent";
export const linkHeader = "space-trace-link";

// ASCII only: these helpers also run in Temporal's deterministic sandbox.
export function tracePayload(value: string) {
  return { metadata: { encoding: Uint8Array.from([98, 105, 110, 97, 114, 121, 47, 112, 108, 97, 105, 110]) },
    data: Uint8Array.from(value, char => char.charCodeAt(0)) };
}

export function traceFromHeaders(headers: Headers, key = traceHeader): string | undefined {
  const data = headers[key]?.data;
  if (!data || data.length !== 55) return undefined;
  const value = String.fromCharCode(...data);
  return parseTraceparent(value) ? value : undefined;
}

export function traceFromArgs(args: readonly unknown[], key = "traceparent"): string | undefined {
  const visit = (value: unknown, depth: number): string | undefined => {
    if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
    const record = value as Record<string, unknown>;
    if (parseTraceparent(record[key])) return record[key] as string;
    if (depth > 0) for (const nested of ["turn", "item", "bridge", "input", "turnInput"]) {
      const found = visit(record[nested], depth - 1);
      if (found) return found;
    }
    return undefined;
  };
  for (const arg of args) {
    const found = visit(arg, 3);
    if (found) return found;
  }
  return undefined;
}

const identifier = /^[A-Za-z0-9:._-]{1,200}$/;
const attributeKeys = new Set([
  "space.workflow_id", "space.workflow_run_id", "space.workflow_type", "space.activity_id",
  "space.activity_type", "space.attempt", "space.outcome", "space.reason_code", "space.category", "space.duration_ms"
]);

/** No arbitrary attributes, free-form errors, arguments, paths or URLs. */
export function safeTraceAttributes(attributes: Readonly<Record<string, unknown>>): Record<string, string | number> {
  const result: Record<string, string | number> = {};
  for (const [key, value] of Object.entries(attributes)) {
    if (!attributeKeys.has(key)) continue;
    if (typeof value === "string" && identifier.test(value)) result[key] = value;
    else if (typeof value === "number" && Number.isFinite(value) && value >= 0) result[key] = value;
  }
  return result;
}

export interface WorkflowTraceSpan {
  traceId: string;
  spanId: string;
  parentSpanId: string | null;
  startedAt: number;
  endedAt: number;
  attributes: Record<string, string | number>;
  error: boolean;
  links: { traceId: string; spanId: string }[];
}

export interface WorkflowTraceSinks extends Sinks {
  spaceTracing: { record(span: WorkflowTraceSpan): void };
}

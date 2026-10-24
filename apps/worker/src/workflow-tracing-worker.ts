import { fileURLToPath } from "node:url";
import type { InjectedSinks } from "@temporalio/worker";
import type { SpanExporter } from "@space/runtime";
import { safeTraceAttributes, type WorkflowTraceSinks } from "./workflow-trace-context.js";

export function workflowTracingOptions(spans: SpanExporter | null) {
  const sinks: InjectedSinks<WorkflowTraceSinks> = {
    spaceTracing: { record: {
      callDuringReplay: false,
      fn(_info, span) {
        try {
          const attributes = safeTraceAttributes(span.attributes);
          spans?.record({ traceId: span.traceId, spanId: span.spanId, parentSpanId: span.parentSpanId,
            name: `workflow ${attributes["space.workflow_type"] ?? "unknown"}`, kind: 1,
            startTimeUnixNano: BigInt(span.startedAt) * 1_000_000n,
            endTimeUnixNano: BigInt(span.endedAt) * 1_000_000n,
            attributes, error: span.error, links: span.links });
        } catch { /* Export must never fail workflow processing. */ }
      }
    } }
  };
  return {
    workflowModules: spans ? [fileURLToPath(new URL("./workflow-tracing.js", import.meta.url))] : [],
    sinks: spans ? sinks : {}
  };
}

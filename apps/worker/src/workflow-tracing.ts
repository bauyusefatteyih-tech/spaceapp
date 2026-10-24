import { ContinueAsNew, proxySinks, workflowInfo, type WorkflowInterceptors, type Headers } from "@temporalio/workflow";
import { formatTraceparent, parseTraceparent } from "@space/contracts/observability";
import { linkHeader, safeTraceAttributes, traceFromArgs, traceFromHeaders, traceHeader, tracePayload, type WorkflowTraceSinks } from "./workflow-trace-context.js";

/** No I/O or random-number consumption inside workflow code. */
export function interceptors(): WorkflowInterceptors {
  const sink = proxySinks<WorkflowTraceSinks>().spaceTracing;
  let current: string | undefined;
  const headers = (original: Headers): Headers => current ? { ...original, [traceHeader]: tracePayload(current) } : original;
  return {
    inbound: [{
      async execute(input, next) {
        const info = workflowInfo();
        const incoming = traceFromHeaders(input.headers) ?? traceFromArgs(input.args) ?? traceFromArgs([info.memo]);
        const parent = parseTraceparent(incoming);
        // Temporal run ids are UUIDs. Reusing their entropy avoids changing the
        // workflow's seeded random sequence (and hence existing command ids).
        const runHex = info.runId.replaceAll("-", "");
        const spanId = runHex.slice(16);
        const traceId = parent?.traceId ?? runHex;
        current = formatTraceparent(traceId, spanId, parent?.flags ?? "01");
        const argumentLink = traceFromArgs(input.args, "traceLink");
        const selectedLink = traceFromHeaders(input.headers, linkHeader) ?? argumentLink;
        const link = parseTraceparent(selectedLink);
        const backgroundRoot = argumentLink === selectedLink && link && link.traceId !== parent?.traceId
          && parseTraceparent(traceFromArgs(input.args))?.parentId === parent?.parentId;
        const startedAt = Date.now();
        let outcome = "COMPLETED";
        try { return await next(input); }
        catch (error) {
          outcome = error instanceof ContinueAsNew ? "CONTINUED_AS_NEW" : "FAILED";
          throw error;
        } finally {
          const endedAt = Date.now();
          sink.record({ traceId, spanId, parentSpanId: backgroundRoot ? null : parent?.parentId ?? null, startedAt, endedAt,
            attributes: safeTraceAttributes({ "space.workflow_id": info.workflowId, "space.workflow_run_id": info.runId,
              "space.workflow_type": info.workflowType, "space.attempt": info.attempt, "space.outcome": outcome,
              "space.duration_ms": Math.max(0, endedAt - startedAt) }), error: outcome === "FAILED",
            links: link ? [{ traceId: link.traceId, spanId: link.parentId }] : [] });
        }
      }
    }],
    outbound: [{
      scheduleActivity: (input, next) => next({ ...input, headers: headers(input.headers) }),
      scheduleLocalActivity: (input, next) => next({ ...input, headers: headers(input.headers) }),
      startChildWorkflowExecution(input, next) {
        const explicit = traceFromArgs(input.options.args ?? []);
        const explicitLink = traceFromArgs(input.options.args ?? [], "traceLink");
        // A supervisor may launch a new mission received on a later request.
        // Preserve that mission's trace; ordinary children nest under this run.
        const differentTrace = explicit && parseTraceparent(explicit)?.traceId !== parseTraceparent(current)?.traceId;
        let childHeaders = headers(input.headers);
        if (differentTrace) childHeaders = { ...childHeaders, [traceHeader]: tracePayload(explicit) };
        const link = explicitLink ?? (differentTrace ? current : undefined);
        if (link) childHeaders = { ...childHeaders, [linkHeader]: tracePayload(link) };
        return next({ ...input, headers: childHeaders });
      },
      continueAsNew(input, next) {
        return next({ ...input, headers: { ...headers(input.headers), ...(current ? { [linkHeader]: tracePayload(current) } : {}) } });
      }
    }]
  };
}

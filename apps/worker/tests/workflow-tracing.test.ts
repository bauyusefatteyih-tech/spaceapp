import { createServer } from "node:http";
import Fastify from "fastify";
import { fileURLToPath } from "node:url";
import { Context } from "@temporalio/activity";
import { Worker, bundleWorkflowCode } from "@temporalio/worker";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { describe, expect, it } from "vitest";
import { createOtlpSpanExporter, type ServerSpan } from "../../../packages/runtime/src/otlp-span-exporter.js";
import { createRequestTraceContext, traceContextForRawRequest, TraceContextRegistry, withTurnTraceparent } from "../../api/src/trace-context.js";
import { createHttpObservability } from "../../api/src/observability.js";
import { createActivityObservabilityInterceptors } from "../src/activity-observability.js";
import { workflowTracingOptions } from "../src/workflow-tracing-worker.js";
import { safeTraceAttributes, traceFromHeaders, tracePayload, traceHeader } from "../src/workflow-trace-context.js";

const path = (relative: string) => fileURLToPath(new URL(relative, import.meta.url));
type CollectedSpan = { traceId: string; spanId: string; parentSpanId?: string; name: string;
  attributes: { key: string; value: { stringValue?: string; intValue?: string } }[];
  links?: { traceId: string; spanId: string }[]; status: { code: number } };
const attribute = (span: CollectedSpan, key: string) => {
  const value = span.attributes.find(a => a.key === key)?.value;
  return value?.stringValue ?? value?.intValue;
};

describe("workflow tracing", () => {
  it("is disabled without an exporter and rejects non-allowlisted data", () => {
    expect(workflowTracingOptions(null)).toEqual({ workflowModules: [], sinks: {} });
    expect(safeTraceAttributes({ prompt: "secret", "space.workflow_type": "/private/path",
      "space.reason_code": "ERROR_CODE", "space.attempt": 2, "space.duration_ms": Infinity }))
      .toEqual({ "space.reason_code": "ERROR_CODE", "space.attempt": 2 });
    expect(traceFromHeaders({ [traceHeader]: tracePayload("invalid") })).toBeUndefined();
  });

  it("exports real Temporal runs, retries, child parents and continuation links, then replays without exporting", async () => {
    const received: CollectedSpan[] = [];
    const collector = createServer(async (req, res) => {
      const parts: Buffer[] = [];
      for await (const part of req) parts.push(Buffer.from(part));
      expect(req.url).toBe("/v1/traces");
      const body = JSON.parse(Buffer.concat(parts).toString());
      received.push(...body.resourceSpans.flatMap((r: { scopeSpans: { spans: CollectedSpan[] }[] }) => r.scopeSpans.flatMap(s => s.spans)));
      res.writeHead(200).end("{}");
    });
    await new Promise<void>(resolve => collector.listen(0, "127.0.0.1", resolve));
    const address = collector.address() as { port: number };
    const endpoint = `http://127.0.0.1:${address.port}`;
    const exporter = createOtlpSpanExporter({ endpoint, serviceName: "space-worker", flushIntervalMs: 60_000 });
    const apiExporter = createOtlpSpanExporter({ endpoint, serviceName: "space-api", flushIntervalMs: 60_000 });
    const app = Fastify();
    const httpObservability = createHttpObservability({ serviceName: "space-api", spanExporter: apiExporter });
    app.addHook("onRequest", async request => httpObservability.onRequest(request));
    app.addHook("onResponse", async (request, reply) => httpObservability.onResponse(request, reply));
    let env: TestWorkflowEnvironment | undefined;
    try {
      await exporter.flush(); // An idle flush must not permanently block later batches.
      // The default is self-contained. A prestarted isolated test server is
      // useful on hosts that exceed the SDK's fixed five-second startup limit.
      const testAddress = process.env.SPACE_TRACE_TEST_TEMPORAL_ADDRESS;
      env = testAddress
        ? await TestWorkflowEnvironment.createFromExistingServer({ address: testAddress })
        : await TestWorkflowEnvironment.createTimeSkipping();
      const workflowBundle = await bundleWorkflowCode({ workflowsPath: path("./fixtures/tracing-workflows.ts"),
        workflowInterceptorModules: [path("../src/workflow-tracing.ts")] });
      const options = workflowTracingOptions(exporter);
      const worker = await Worker.create({ connection: env.nativeConnection, taskQueue: "trace-proof", workflowBundle,
        sinks: options.sinks, interceptors: { activity: [createActivityObservabilityInterceptors({ spans: exporter })] },
        activities: { attempt: async () => {
          if (Context.current().info.attempt === 1) throw new Error("PRIVATE ERROR /home/private/token SECRET");
          return "PRIVATE OUTPUT";
        } } });
      let api = createRequestTraceContext(undefined);
      app.post("/test/turn", async request => {
        api = traceContextForRawRequest(request.raw);
        const registry = new TraceContextRegistry();
        registry.remember(request.id, api);
        const turn = withTurnTraceparent({ traceId: request.id, prompt: "PRIVATE PROMPT https://secret.invalid/token" }, registry);
        return env!.client.workflow.execute("tracingRoot", { workflowId: "trace-root", taskQueue: "trace-proof", args: [turn] });
      });
      await worker.runUntil(async () => {
        const response = await app.inject({ method: "POST", url: "/test/turn" });
        expect(response.statusCode).toBe(200);
        expect(response.body).toBe("complete");
        const background = createRequestTraceContext(undefined);
        expect(await env!.client.workflow.execute("tracingBackground", { workflowId: "trace-background", taskQueue: "trace-proof",
          args: [{ traceparent: background.traceparent, traceLink: api.traceparent }] })).toBe("background complete");
      });
      await exporter.flush();
      await apiExporter.flush();
      const workflows = received.filter(s => s.name.startsWith("workflow "));
      const roots = workflows.filter(s => s.name === "workflow tracingRoot");
      expect(roots).toHaveLength(2);
      const first = roots.find(s => attribute(s, "space.outcome") === "CONTINUED_AS_NEW")!;
      const second = roots.find(s => attribute(s, "space.outcome") === "COMPLETED")!;
      expect(first).toMatchObject({ traceId: api.w3cTraceId, parentSpanId: api.spanId });
      expect(second.links).toContainEqual({ traceId: first.traceId, spanId: first.spanId });
      const children = workflows.filter(s => s.name === "workflow tracingChild");
      expect(children).toHaveLength(2);
      for (const child of children) expect(roots.some(root => root.spanId === child.parentSpanId && root.traceId === child.traceId)).toBe(true);
      const background = workflows.find(s => s.name === "workflow tracingBackground")!;
      expect(background.traceId).not.toBe(api.w3cTraceId);
      expect(background.parentSpanId).toBeUndefined();
      expect(background.links).toContainEqual({ traceId: api.w3cTraceId, spanId: api.spanId });
      const activities = received.filter(s => s.name === "activity attempt");
      expect(activities).toHaveLength(10);
      expect(new Set(activities.map(s => s.spanId)).size).toBe(10);
      for (const activity of activities) {
        expect(workflows.some(w => w.spanId === activity.parentSpanId && w.traceId === activity.traceId)).toBe(true);
        const attempt = attribute(activity, "space.attempt");
        expect(attribute(activity, "space.outcome")).toBe(attempt === "1" ? "FAILED" : "COMPLETED");
        if (attempt === "1") expect(attribute(activity, "space.reason_code")).toBe("ACTIVITY_ERROR");
      }
      expect(JSON.stringify(received)).not.toMatch(/PRIVATE|secret\.invalid|home\/private|SECRET/);
      let replayExports = 0;
      const replayExporter = { ...exporter, record: (_span: ServerSpan) => { replayExports++; } };
      for (const workflow of workflows) {
        const history = await env.client.workflow.getHandle(attribute(workflow, "space.workflow_id")!, attribute(workflow, "space.workflow_run_id")!).fetchHistory();
        await Worker.runReplayHistory({ workflowBundle, sinks: workflowTracingOptions(replayExporter).sinks }, history);
      }
      expect(replayExports).toBe(0);

      // Histories recorded before tracing was enabled must still replay. The
      // fixture also starts children with SDK-generated ids, guarding the PRNG.
      const plainBundle = await bundleWorkflowCode({ workflowsPath: path("./fixtures/tracing-workflows.ts") });
      const plainWorker = await Worker.create({ connection: env.nativeConnection, taskQueue: "trace-disabled", workflowBundle: plainBundle,
        activities: { attempt: async () => "ok" } });
      const legacyHistory = await plainWorker.runUntil(async () => {
        const handle = await env!.client.workflow.start("tracingRoot", { workflowId: "trace-legacy", taskQueue: "trace-disabled",
          args: [{ traceparent: api.traceparent, prompt: "PRIVATE" }] });
        expect(await handle.result()).toBe("complete");
        return env!.client.workflow.getHandle("trace-legacy", handle.firstExecutionRunId).fetchHistory();
      });
      await Worker.runReplayHistory({ workflowBundle, sinks: workflowTracingOptions(replayExporter).sinks }, legacyHistory);
      expect(replayExports).toBe(0);

      const unavailable = createOtlpSpanExporter({ endpoint: "http://127.0.0.1:1", serviceName: "outage-test", timeoutMs: 100 });
      try {
        const outageWorker = await Worker.create({ connection: env.nativeConnection, taskQueue: "trace-outage", workflowBundle,
          sinks: workflowTracingOptions(unavailable).sinks,
          interceptors: { activity: [createActivityObservabilityInterceptors({ spans: unavailable })] },
          activities: { attempt: async () => "ok" } });
        await outageWorker.runUntil(async () => {
          expect(await env!.client.workflow.execute("tracingBackground", { workflowId: "trace-outage", taskQueue: "trace-outage",
            args: [{ traceparent: api.traceparent, traceLink: api.traceparent }] })).toBe("background complete");
        });
        await unavailable.flush();
        expect(unavailable.stats().failed).toBe(2);
      } finally { await unavailable.close(); }
    } finally {
      await app.close();
      httpObservability.close();
      await env?.teardown();
      await exporter.close();
      await apiExporter.close();
      await new Promise<void>((resolve, reject) => collector.close(error => error ? reject(error) : resolve()));
    }
  }, 180_000);
});

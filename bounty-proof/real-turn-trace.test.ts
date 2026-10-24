// Fork-only acceptance evidence, copied into apps/api/tests on the pinned PR head.
// Real HTTP route, Temporal server, production workflow and dummy activities.
// In-memory API storage; no model provider, operator profile or wallet involved.
import { fileURLToPath } from "node:url";
import { Worker, bundleWorkflowCode } from "@temporalio/worker";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { expect, it, vi } from "vitest";
import { InMemorySpaceStore, otlpSpanExporterFromEnv } from "@space/runtime";
import { createApp } from "../src/app.js";
import { TemporalTurnStarter } from "../src/turns.js";
import { recordDummyTurnStarted, recordDummyTurnCompleted } from "../../worker/src/activities.js";
import { workflowTracingOptions } from "../../worker/src/workflow-tracing-worker.js";
import { createActivityObservabilityInterceptors } from "../../worker/src/activity-observability.js";

it("captures the actual turn API through production dummy workflow and activities in Jaeger", async () => {
  const path = (relative: string) => fileURLToPath(new URL(relative, import.meta.url));
  vi.stubEnv("SPACE_OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318");
  vi.stubEnv("SPACE_DATABASE_URL", "");
  const originalFetch = globalThis.fetch;
  const batches: any[] = [];
  const fetchSpy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const response = await originalFetch(input, init);
    if (String(input) === "http://127.0.0.1:4318/v1/traces" && response.ok && typeof init?.body === "string") {
      batches.push(JSON.parse(init.body));
    }
    return response;
  });
  let env: TestWorkflowEnvironment | undefined;
  let app: Awaited<ReturnType<typeof createApp>> | undefined;
  const exporter = otlpSpanExporterFromEnv(process.env, "space-worker")!;
  try {
    env = await TestWorkflowEnvironment.createTimeSkipping();
    const workflowBundle = await bundleWorkflowCode({
      workflowsPath: path("../../worker/src/workflows.ts"),
      workflowInterceptorModules: [path("../../worker/src/workflow-tracing.ts")]
    });
    const tracing = workflowTracingOptions(exporter);
    const worker = await Worker.create({
      connection: env.nativeConnection, taskQueue: "actual-turn-proof", workflowBundle,
      sinks: tracing.sinks,
      interceptors: { activity: [createActivityObservabilityInterceptors({ spans: exporter })] },
      activities: { recordDummyTurnStarted, recordDummyTurnCompleted }
    });
    const store = new InMemorySpaceStore();
    const room = store.createRoom({ name: "Isolated trace proof", initialPaneCount: 1 }, "trace:proof-seed");
    const pane = store.listPanes(room.id)[0]!;
    app = await createApp({
      store,
      turnStarter: new TemporalTurnStarter({
        address: "isolated-test-server", namespace: "default", taskQueue: "actual-turn-proof",
        clientFactory: async () => ({ client: env!.client, close: async () => undefined })
      }),
      auth: { sessionSecret: "isolated-proof-not-a-production-secret", devLogin: true, secureCookies: false }
    });
    const login = await app.inject({ method: "POST", url: "/api/auth/login", payload: { email: "space@space.local", password: "space-dev" } });
    expect(login.statusCode).toBe(200);
    const cookie = `space_session=${login.cookies.find(entry => entry.name === "space_session")!.value}`;
    const csrfToken = (await app.inject({ method: "GET", url: "/api/auth/csrf", headers: { cookie } })).json().csrfToken;
    const url = await app.listen({ port: 0, host: "127.0.0.1" });
    let turn: any;
    await worker.runUntil(async () => {
      const response = await fetch(`${url}/api/turns`, {
        method: "POST", headers: { cookie, "x-space-csrf-token": csrfToken, "content-type": "application/json" },
        body: JSON.stringify({ roomId: room.id, paneId: pane.id, runtime: "DUMMY_TEMPORAL", prompt: "PRIVATE_PROOF_PROMPT_MUST_NOT_EXPORT" })
      });
      turn = await response.json();
      expect(response.status, JSON.stringify(turn)).toBe(200);
      const result = await env!.client.workflow.getHandle(turn.workflowId).result();
      expect(result.status).toBe("COMPLETED");
    });
    await exporter.flush();
    await app.close();
    app = undefined;
    const spans = () => batches.flatMap(batch => batch.resourceSpans.flatMap((resource: any) => resource.scopeSpans.flatMap((scope: any) => scope.spans)));
    await vi.waitFor(() => expect(spans().some((span: any) => span.name === "POST /api/turns")).toBe(true), { timeout: 10000 });
    const apiSpan = spans().find((span: any) => span.name === "POST /api/turns");
    const workflow = spans().find((span: any) => span.name === "workflow dummyTurnWorkflow");
    expect(workflow).toMatchObject({ traceId: apiSpan.traceId, parentSpanId: apiSpan.spanId });
    const activities = spans().filter((span: any) => span.name.startsWith("activity "));
    expect(activities.map((span: any) => span.name).sort()).toEqual(["activity recordDummyTurnCompleted", "activity recordDummyTurnStarted"]);
    for (const activity of activities) expect(activity).toMatchObject({ traceId: apiSpan.traceId, parentSpanId: workflow.spanId });
    expect(JSON.stringify(batches)).not.toContain("PRIVATE_PROOF_PROMPT_MUST_NOT_EXPORT");
    let jaeger: any;
    await vi.waitFor(async () => {
      jaeger = await (await originalFetch(`http://127.0.0.1:16686/api/traces/${apiSpan.traceId}`)).json();
      expect(jaeger.data?.[0]?.spans).toHaveLength(4);
    }, { timeout: 15000 });
    console.log("ACTUAL_TURN_PROOF_JSON=" + JSON.stringify({
      sourceHead: "5979fb3abdcc7e404edc41e8118bd3c6c8a25916",
      runtime: "DUMMY_TEMPORAL", productionHttpRoute: "/api/turns", traceId: apiSpan.traceId,
      workflowId: turn.workflowId, spanCount: jaeger.data[0].spans.length,
      limitations: ["In-memory API storage", "Dummy runtime; no model-provider execution", "Temporal SDK test server"],
      batches, jaeger
    }));
  } finally {
    await app?.close();
    await env?.teardown();
    await exporter.close();
    fetchSpy.mockRestore();
    vi.unstubAllEnvs();
  }
}, 180000);

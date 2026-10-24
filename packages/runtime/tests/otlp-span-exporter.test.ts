import { describe, expect, it } from "vitest";
import { createOtlpSpanExporter, otlpTraceRequest, type ServerSpan } from "../src/otlp-span-exporter.js";

const span: ServerSpan = {
  traceId: "a".repeat(32), spanId: "b".repeat(16), parentSpanId: null, name: "workflow test",
  startTimeUnixNano: 1n, endTimeUnixNano: 2n, attributes: {}, error: false, kind: 1
};

describe("shared OTLP exporter", () => {
  it("continues exporting after idle and concurrent flushes", async () => {
    const bodies: string[] = [];
    const exporter = createOtlpSpanExporter({ endpoint: "http://localhost:4318", serviceName: "test",
      fetch: async (_url, init) => { bodies.push(init.body); return { ok: true }; } });
    try {
      await Promise.all([exporter.flush(), exporter.flush()]);
      exporter.record(span);
      await exporter.flush();
      await exporter.flush();
      exporter.record(span);
      await exporter.flush();
      expect(bodies).toHaveLength(2);
      expect(exporter.stats()).toEqual({ exported: 2, dropped: 0, failed: 0 });
    } finally { await exporter.close(); }
  });

  it("serializes valid OTLP links and rejects malformed ids", () => {
    const link = { traceId: "c".repeat(32), spanId: "d".repeat(16) };
    const body = otlpTraceRequest([{ ...span, links: [link, { traceId: "secret", spanId: "bad" }] }], "test");
    expect(body.resourceSpans[0]!.scopeSpans[0]!.spans[0]!.links).toEqual([link]);
  });

  it("bounds the queue and counts collector failures without rejecting", async () => {
    const exporter = createOtlpSpanExporter({ endpoint: "http://localhost:4318", serviceName: "test", maxQueue: 2,
      fetch: async () => { throw new Error("collector unavailable"); } });
    try {
      for (let i = 0; i < 5; i++) exporter.record(span);
      await expect(exporter.flush()).resolves.toBeUndefined();
      expect(exporter.stats()).toEqual({ exported: 0, dropped: 3, failed: 2 });
    } finally { await exporter.close(); }
  });
});

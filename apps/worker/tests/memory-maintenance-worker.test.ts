import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  run: vi.fn(),
  close: vi.fn(),
  stopReloadWatch: vi.fn(),
  exporter: vi.fn()
}));

vi.mock("@temporalio/worker", () => ({
  NativeConnection: { connect: vi.fn().mockResolvedValue({}) },
  Worker: { create: vi.fn(async () => ({ run: mocks.run })) }
}));
vi.mock("@space/runtime", () => ({ otlpSpanExporterFromEnv: mocks.exporter }));
vi.mock("../src/memory-maintenance-activities.js", () => ({}));
vi.mock("../src/activity-observability.js", () => ({ createActivityObservabilityInterceptors: vi.fn() }));
vi.mock("../src/workflow-tracing-worker.js", () => ({ workflowTracingOptions: () => ({}) }));
vi.mock("../src/deployed-code-reload.js", () => ({
  reloadWhenServicesCommitChanges: () => mocks.stopReloadWatch
}));

describe("maintenance worker shutdown", () => {
  let originalExitCode: typeof process.exitCode;

  beforeEach(() => {
    vi.resetModules();
    vi.resetAllMocks();
    originalExitCode = process.exitCode;
    process.exitCode = undefined;
    vi.stubEnv("SPACE_MEMORY_GRAPH_ENABLED", "true");
    vi.stubEnv("SPACE_MEMORY_MAINTENANCE_ENABLED", "true");
    vi.spyOn(console, "log").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.close.mockResolvedValue(undefined);
    mocks.exporter.mockReturnValue({ close: mocks.close });
  });

  afterEach(() => {
    process.exitCode = originalExitCode;
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it.each([false, true])("cleans up when worker.run rejects: %s", async (fails) => {
    if (fails) mocks.run.mockRejectedValue(new Error("worker failed"));
    else mocks.run.mockResolvedValue(undefined);

    await import("../src/memory-maintenance-worker.js");

    expect(mocks.run).toHaveBeenCalledOnce();
    expect(mocks.stopReloadWatch).toHaveBeenCalledOnce();
    expect(mocks.close).toHaveBeenCalledOnce();
    expect(process.exitCode).toBe(fails ? 1 : undefined);
    if (fails) {
      expect(console.error).toHaveBeenCalledWith(expect.stringContaining('"error":"worker failed"'));
    }
  });

  it("preserves failure handling when tracing is disabled", async () => {
    mocks.exporter.mockReturnValue(undefined);
    mocks.run.mockRejectedValue(new Error("worker failed"));

    await import("../src/memory-maintenance-worker.js");

    expect(mocks.stopReloadWatch).toHaveBeenCalledOnce();
    expect(mocks.close).not.toHaveBeenCalled();
    expect(process.exitCode).toBe(1);
  });
});

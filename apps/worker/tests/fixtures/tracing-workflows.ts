import { continueAsNew, executeChild, proxyActivities } from "@temporalio/workflow";

const activities = proxyActivities<{ attempt(): Promise<string> }>({
  startToCloseTimeout: "10 seconds", retry: { initialInterval: "1 second", maximumAttempts: 2 }
});

export async function tracingChild(): Promise<string> {
  return activities.attempt();
}

export async function tracingRoot(input: { traceparent: string; prompt: string }, generation = 0): Promise<string> {
  await activities.attempt();
  await executeChild(tracingChild);
  if (generation === 0) return continueAsNew<typeof tracingRoot>(input, 1);
  return "complete";
}

export async function tracingBackground(input: { traceparent: string; traceLink: string }): Promise<string> {
  await activities.attempt();
  return "background complete";
}

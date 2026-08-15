import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

import { MatrixTransportController, loadConfig, type PreparedInbound } from "./core.ts";
import type { MatrixInboundEvent } from "./ipc.ts";
import { request } from "./ipc.ts";

export default function matrixTransportAgent(pi: ExtensionAPI): void {
  const config = loadConfig(process.env);
  if (!config.enabled) return;
  const extensionDir = path.dirname(fileURLToPath(import.meta.url));
  const topicHelperPath = config.topicHelperPath || path.resolve(extensionDir, "../bin/matrix-topic-kb.py");

  let currentContext: ExtensionContext | undefined;
  let timer: NodeJS.Timeout | undefined;
  let generation = 0;
  const controller = new MatrixTransportController(config, {
    ipc: (payload) => request(config.socketPath, payload),
    isIdle: () => currentContext?.isIdle() === true,
    inject: (prompt) => pi.sendUserMessage(prompt),
    prepareInbound: config.topicEnabled
      ? (event) => prepareTopicInbound(topicHelperPath, event)
      : undefined,
    log: (level, message) => console[level](`[pi-matrix-transport-agent] ${message}`),
  });

  pi.on("session_start", (_event, ctx) => {
    generation += 1;
    const ownGeneration = generation;
    currentContext = ctx;
    if (timer) clearInterval(timer);
    timer = setInterval(() => {
      if (ownGeneration !== generation) return;
      void controller.tick();
    }, config.pollMs);
    timer.unref?.();
    void controller.tick();
  });

  pi.on("agent_end", (event) => {
    controller.captureAgentEnd(Array.from(event.messages ?? []));
  });

  pi.on("agent_settled", async () => {
    await controller.onAgentSettled();
  });

  pi.on("session_shutdown", async () => {
    generation += 1;
    currentContext = undefined;
    if (timer) clearInterval(timer);
    timer = undefined;
    await controller.onSessionShutdown();
  });
}

async function prepareTopicInbound(helperPath: string, event: MatrixInboundEvent): Promise<PreparedInbound> {
  const result = await runTopicHelper(helperPath, event.event_id, event.body);
  if (!result.ok) {
    return { directAnswer: `Project topic routing blocked: ${result.error || "unknown error"}. No capture was written.` };
  }
  if (typeof result.directAnswer === "string" && result.directAnswer.trim()) {
    return { directAnswer: result.directAnswer.trim() };
  }
  if (typeof result.prompt === "string" && result.prompt.trim()) {
    return { prompt: result.prompt.trim() };
  }
  return { directAnswer: "Project topic routing blocked: helper returned no action. No capture was written." };
}

type TopicHelperResult = {
  ok: boolean;
  error?: string;
  directAnswer?: string;
  prompt?: string;
};

async function runTopicHelper(helperPath: string, eventId: string, body: string): Promise<TopicHelperResult> {
  return await new Promise<TopicHelperResult>((resolve) => {
    const child = spawn(helperPath, ["prepare", "--event-id", eventId], {
      env: process.env,
      stdio: ["pipe", "pipe", "ignore"],
    });
    let stdout = "";
    let settled = false;
    const timer = setTimeout(() => finish({ ok: false, error: "OpenKnowledge topic helper timed out" }), 20_000);

    const finish = (result: TopicHelperResult): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.kill("SIGKILL");
      resolve(result);
    };

    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout += chunk;
      if (stdout.length > 65_536) finish({ ok: false, error: "OpenKnowledge topic helper response too large" });
    });
    child.on("error", () => finish({ ok: false, error: "OpenKnowledge topic helper unavailable" }));
    child.on("close", () => {
      try {
        const parsed = JSON.parse(stdout.trim()) as TopicHelperResult;
        finish(parsed);
      } catch {
        finish({ ok: false, error: "OpenKnowledge topic helper returned invalid output" });
      }
    });
    child.stdin.end(body);
  });
}

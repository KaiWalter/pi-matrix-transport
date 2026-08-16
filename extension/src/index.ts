import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

import {
  MatrixTransportController,
  buildPrompt,
  loadConfig,
  parseMatrixSlashCommand,
  type PreparedInbound,
} from "./core.ts";
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
    prepareInbound: (event) => prepareInboundEvent({
      pi,
      config,
      event,
      context: currentContext,
      topicHelperPath,
    }),
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

type MatrixModelTier = "power" | "balanced" | "eco";

type MatrixPrepareOptions = {
  pi: ExtensionAPI;
  config: ReturnType<typeof loadConfig>;
  event: MatrixInboundEvent;
  context?: ExtensionContext;
  topicHelperPath: string;
};

type TierConfig = {
  power?: string;
  balanced?: string;
  eco?: string;
};

type ModelRegistryLike = {
  find?: (provider: string, modelId: string) => unknown;
};

async function prepareInboundEvent(options: MatrixPrepareOptions): Promise<PreparedInbound> {
  const slashCommand = parseMatrixSlashCommand(options.event.body, options.event.kind);
  if (slashCommand) {
    return await handleSlashCommand(options.pi, options.context, slashCommand);
  }
  if (options.config.topicEnabled) {
    return await prepareTopicInbound(options.topicHelperPath, options.event);
  }
  return { prompt: buildPrompt(options.event.body, options.event.kind, options.config.promptTag) };
}

async function handleSlashCommand(
  pi: ExtensionAPI,
  context: ExtensionContext | undefined,
  command: "reload" | "new" | MatrixModelTier,
): Promise<PreparedInbound> {
  if (command === "reload" || command === "new") {
    return {
      directAnswer: `Running /${command}.`,
      afterSend: async () => {
        const ok = await triggerNativeSlashCommand(pi, command);
        if (!ok) {
          console.warn(`[pi-matrix-transport-agent] failed to trigger /${command} after send`);
        }
      },
    };
  }

  const lane = laneName();
  const tiers = loadTierConfig(lane);
  const modelRef = command === "power" ? tiers.power : command === "balanced" ? tiers.balanced : tiers.eco;
  if (!modelRef) {
    return { directAnswer: `No /${command} model configured for lane ${lane}.` };
  }

  const parsed = parseModelRef(modelRef);
  const modelRegistry = (context as unknown as { modelRegistry?: ModelRegistryLike })?.modelRegistry;
  const model = parsed && modelRegistry?.find
    ? modelRegistry.find(parsed.provider, parsed.modelId)
    : undefined;
  if (!model) {
    return { directAnswer: `Could not resolve model ${modelRef} for /${command}.` };
  }

  try {
    const ok = await pi.setModel(model as never);
    if (ok === false) {
      return { directAnswer: `Failed to switch /${command}: no API key available.` };
    }
    return { directAnswer: `${capitalize(command)} mode enabled (${modelRef}).` };
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    return { directAnswer: `Failed to switch /${command}: ${reason}` };
  }
}

async function currentHerdrPaneId(pi: ExtensionAPI): Promise<string | undefined> {
  try {
    const result = await pi.exec("herdr", ["pane", "current"]);
    const payload = JSON.parse(result.stdout ?? "{}") as {
      result?: { pane?: { pane_id?: string } };
    };
    const paneId = String(payload?.result?.pane?.pane_id ?? "").trim();
    return paneId || undefined;
  } catch {
    return undefined;
  }
}

async function triggerNativeSlashCommand(pi: ExtensionAPI, command: "reload" | "new"): Promise<boolean> {
  const pane = await currentHerdrPaneId(pi);
  if (pane) {
    try {
      await pi.exec("herdr", ["pane", "send-text", pane, `/${command}`]);
      await pi.exec("herdr", ["pane", "send-keys", pane, "Enter"]);
      return true;
    } catch {
      // fallback below
    }
  }

  try {
    pi.sendUserMessage(`/${command}`);
    return true;
  } catch {
    return false;
  }
}

function laneName(): string {
  const fromEnv = process.env.PI_CODING_AGENT_DIR || process.cwd();
  return path.basename(fromEnv.replace(/\/+$/, "")) || "unknown";
}

function laneLookupCandidates(lane: string): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  const push = (value: string): void => {
    const v = value.trim();
    if (!v || seen.has(v)) return;
    seen.add(v);
    out.push(v);
  };

  push(lane);
  const suffixes = ["-herdr-matrix", "-herdr-telegram", "-herdr-dapr", "-telegram", "-dapr", "-bus"];
  let current = lane;
  for (const suffix of suffixes) {
    if (current.endsWith(suffix)) {
      current = current.slice(0, -suffix.length);
      push(current);
    }
  }
  return out;
}

function loadTierConfig(lane: string): TierConfig {
  const envPower = process.env.PI_MATRIX_MODEL_POWER || process.env.PI_TELEGRAM_MODEL_POWER;
  const envBalanced = process.env.PI_MATRIX_MODEL_BALANCED || process.env.PI_TELEGRAM_MODEL_BALANCED;
  const envEco = process.env.PI_MATRIX_MODEL_ECO || process.env.PI_TELEGRAM_MODEL_ECO;

  let filePower: string | undefined;
  let fileBalanced: string | undefined;
  let fileEco: string | undefined;
  try {
    const cfgPath = path.join(os.homedir(), ".pi", "shared", "data", "model-fidelity", "tiers.json");
    const raw = JSON.parse(fs.readFileSync(cfgPath, "utf-8")) as Record<string, unknown>;
    for (const key of laneLookupCandidates(lane)) {
      const laneCfg = raw[key];
      if (!laneCfg || typeof laneCfg !== "object") continue;
      const cfg = laneCfg as Record<string, unknown>;
      if (!filePower && typeof cfg.power === "string") filePower = cfg.power;
      if (!fileBalanced && typeof cfg.balanced === "string") fileBalanced = cfg.balanced;
      if (!fileEco && typeof cfg.eco === "string") fileEco = cfg.eco;
      if (filePower && fileBalanced && fileEco) break;
    }
  } catch {
    // env overrides may still provide tier mappings
  }

  return {
    power: envPower || filePower,
    balanced: envBalanced || fileBalanced,
    eco: envEco || fileEco,
  };
}

function parseModelRef(ref: string): { provider: string; modelId: string } | undefined {
  const idx = ref.indexOf("/");
  if (idx <= 0 || idx >= ref.length - 1) return undefined;
  return { provider: ref.slice(0, idx), modelId: ref.slice(idx + 1) };
}

function capitalize(value: string): string {
  return value.length > 0 ? `${value.charAt(0).toUpperCase()}${value.slice(1)}` : value;
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

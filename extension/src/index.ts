import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

import { MatrixTransportController, loadConfig } from "./core.ts";
import { request } from "./ipc.ts";

export default function matrixTransportAgent(pi: ExtensionAPI): void {
  const config = loadConfig(process.env);
  if (!config.enabled) return;

  let currentContext: ExtensionContext | undefined;
  let timer: NodeJS.Timeout | undefined;
  let generation = 0;
  const controller = new MatrixTransportController(config, {
    ipc: (payload) => request(config.socketPath, payload),
    isIdle: () => currentContext?.isIdle() === true,
    inject: (prompt) => pi.sendUserMessage(prompt),
    log: (level, message) => console[level](`[pi-matrix-transport] ${message}`),
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

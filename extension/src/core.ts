import type { InboundEvent, IpcRequest, IpcResponse } from "./ipc.ts";

export type MatrixTransportConfig = {
  enabled: boolean;
  socketPath: string;
  pollMs: number;
};

export type MatrixTransportDeps = {
  ipc: (request: IpcRequest) => Promise<IpcResponse>;
  isIdle: () => boolean;
  inject: (prompt: string) => void;
  log: (level: "info" | "warn", message: string) => void;
};

type ActiveTurn = {
  event: InboundEvent;
  answer?: string;
};

export function loadConfig(env: NodeJS.ProcessEnv): MatrixTransportConfig {
  const enabled = env.PI_MATRIX_XO_ENABLED === "1";
  const socketPath = env.PI_MATRIX_XO_SOCKET?.trim() || `${env.XDG_RUNTIME_DIR || "/tmp"}/pi-matrix-xo.sock`;
  const parsedPoll = Number.parseInt(env.PI_MATRIX_XO_POLL_MS || "1000", 10);
  const pollMs = Number.isFinite(parsedPoll) ? Math.min(30_000, Math.max(250, parsedPoll)) : 1000;
  return { enabled, socketPath, pollMs };
}

export class MatrixTransportController {
  private active?: ActiveTurn;
  private polling = false;
  private readonly deps: MatrixTransportDeps;

  constructor(deps: MatrixTransportDeps) {
    this.deps = deps;
  }

  async tick(): Promise<void> {
    if (this.polling) return;
    this.polling = true;
    try {
      if (this.active?.answer) {
        await this.flushAnswer();
        return;
      }
      if (this.active || !this.safeIsIdle()) return;
      const response = await this.deps.ipc({ op: "claim" });
      if (!response.ok || !response.event) return;
      if (!validInbound(response.event)) {
        this.deps.log("warn", "Matrix sidecar returned an invalid claimed event");
        return;
      }
      this.active = { event: response.event };
      try {
        this.deps.inject(buildPrompt(response.event.body, response.event.kind));
        this.deps.log("info", "Injected one Matrix canary turn into XO");
      } catch {
        await this.releaseActive();
        this.deps.log("warn", "Failed to inject Matrix canary turn into XO");
      }
    } catch {
      this.deps.log("warn", "Matrix canary sidecar is unavailable");
    } finally {
      this.polling = false;
    }
  }

  captureAgentEnd(messages: unknown[]): void {
    if (!this.active || this.active.answer) return;
    const answer = extractFinalAssistantText(messages);
    if (answer) this.active.answer = answer;
  }

  async onAgentSettled(): Promise<void> {
    if (!this.active?.answer) return;
    await this.flushAnswer();
  }

  async onSessionShutdown(): Promise<void> {
    await this.releaseActive();
  }

  hasActiveTurn(): boolean {
    return this.active !== undefined;
  }

  private async flushAnswer(): Promise<void> {
    const active = this.active;
    if (!active?.answer) return;
    try {
      const response = await this.deps.ipc({
        op: "send",
        event_id: active.event.event_id,
        idempotency_key: `xo-reply:${active.event.event_id}`,
        body: active.answer,
      });
      if (!response.ok) throw new Error("send rejected");
      this.active = undefined;
      this.deps.log("info", "Delivered one XO answer to the Matrix canary room");
    } catch {
      this.deps.log("warn", "Matrix canary answer remains queued for retry");
    }
  }

  private async releaseActive(): Promise<void> {
    const eventId = this.active?.event.event_id;
    this.active = undefined;
    if (!eventId) return;
    try {
      await this.deps.ipc({ op: "release", event_id: eventId });
    } catch {
      // The sidecar requeues all claimed rows at restart; avoid logging identifiers.
    }
  }

  private safeIsIdle(): boolean {
    try {
      return this.deps.isIdle();
    } catch {
      return false;
    }
  }
}

export function buildPrompt(body: string, kind: InboundEvent["kind"] = "text"): string {
  return kind === "voice"
    ? `[matrix voice]\n${body.trim()}`
    : `[matrix]\n${body.trim()}`;
}

export function extractFinalAssistantText(messages: unknown[]): string | undefined {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index] as { role?: unknown; content?: unknown };
    if (message?.role !== "assistant") continue;
    if (typeof message.content === "string") {
      const text = message.content.trim();
      if (text) return text;
    }
    if (!Array.isArray(message.content)) continue;
    const text = message.content
      .filter((part): part is { type: "text"; text: string } => {
        if (!part || typeof part !== "object") return false;
        const candidate = part as { type?: unknown; text?: unknown };
        return candidate.type === "text" && typeof candidate.text === "string";
      })
      .map((part) => part.text)
      .join("\n")
      .trim();
    if (text) return text;
  }
  return undefined;
}

function validInbound(event: InboundEvent): boolean {
  return typeof event.event_id === "string"
    && event.event_id.length > 0
    && typeof event.body === "string"
    && event.body.trim().length > 0
    && (event.kind === "text" || event.kind === "voice")
    && [...event.body].length <= 16_000;
}

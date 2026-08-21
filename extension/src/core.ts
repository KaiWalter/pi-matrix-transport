import type { MatrixInboundEvent, MatrixIpcRequest, MatrixIpcResponse } from "./ipc.ts";

export type MatrixAgentTransportConfig = {
  enabled: boolean;
  socketPath: string;
  pollMs: number;
  idempotencyPrefix: string;
  promptTag: string;
  laneLabel: string;
  roomId?: string;
};

export type MatrixUserMessage = string | Array<
  | { type: "text"; text: string }
  | { type: "image"; mimeType: string; data: string }
>;

export type PreparedInbound = {
  prompt?: MatrixUserMessage;
  directAnswer?: string;
  afterSend?: () => Promise<void> | void;
};

export type MatrixTransportDeps = {
  ipc: (request: MatrixIpcRequest) => Promise<MatrixIpcResponse>;
  isIdle: () => boolean;
  inject: (prompt: MatrixUserMessage) => void;
  prepareInbound?: (event: MatrixInboundEvent) => Promise<PreparedInbound>;
  log: (level: "info" | "warn", message: string) => void;
};

type ActiveTurn = {
  event: MatrixInboundEvent;
  answer?: string;
  prompt?: MatrixUserMessage;
  afterSend?: () => Promise<void> | void;
  modelRetryPending?: boolean;
  modelRetryCount?: number;
  modelNextRetryAt?: number;
  activityStartedAt: number;
  activityStatusEventId?: string;
  activityLongRunning?: boolean;
};

const RETRYABLE_LLM_ERROR = "Error: Unknown error (no error details in response)";
const LONG_RUNNING_ACTIVITY_MS = 5000;

export function loadConfig(env: NodeJS.ProcessEnv): MatrixAgentTransportConfig {
  const enabled = env.PI_MATRIX_AGENT_ENABLED === "1";
  const socketPath = env.PI_MATRIX_AGENT_SOCKET?.trim() || "";
  if (enabled && !socketPath) {
    throw new Error("PI_MATRIX_AGENT_SOCKET is required when the Matrix extension is enabled");
  }
  const parsedPoll = Number.parseInt(env.PI_MATRIX_AGENT_POLL_MS || "1000", 10);
  const pollMs = Number.isFinite(parsedPoll) ? Math.min(30000, Math.max(250, parsedPoll)) : 1000;
  const idempotencyPrefix = env.PI_MATRIX_AGENT_IDEMPOTENCY_PREFIX?.trim() || "matrix-reply";
  const promptTag = env.PI_MATRIX_AGENT_PROMPT_TAG?.trim() || "matrix";
  const laneLabel = env.PI_MATRIX_AGENT_LABEL?.trim() || "Matrix lane";
  const roomId = env.PI_MATRIX_AGENT_ROOM_ID?.trim() || undefined;
  return { enabled, socketPath, pollMs, idempotencyPrefix, promptTag, laneLabel, roomId };
}

export class MatrixTransportController {
  private active?: ActiveTurn;
  private polling = false;
  private unavailableSince?: number;
  private lastUnavailableLogAt = 0;
  private readonly unavailableLogIntervalMs = 30000;
  private answerRetrySince?: number;
  private answerRetryCount = 0;
  private answerRetryReason = "send failed";
  private answerNextAttemptAt = 0;
  private lastAnswerRetryLogAt = 0;
  private readonly answerRetryLogIntervalMs = 600000;
  private readonly deps: MatrixTransportDeps;
  private readonly config: MatrixAgentTransportConfig;

  constructor(config: MatrixAgentTransportConfig, deps: MatrixTransportDeps) {
    this.config = config;
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
      if (this.active) {
        await this.refreshActivity(this.active);
        if (this.active.modelRetryPending) await this.retryModelTurnIfReady();
        return;
      }
      if (!this.safeIsIdle()) return;
      const claimRequest = this.config.roomId
        ? { op: "claim", room_id: this.config.roomId } as const
        : { op: "claim" } as const;
      const response = await this.deps.ipc(claimRequest);
      this.markSidecarAvailable();
      if (!response.ok || !response.event) return;
      if (!validInbound(response.event)) {
        this.deps.log("warn", `${this.config.laneLabel}: sidecar returned an invalid claimed event`);
        return;
      }
      this.active = { event: response.event, activityStartedAt: Date.now() };
      await this.startActivity(this.active);
      try {
        const prepared = this.deps.prepareInbound
          ? await this.deps.prepareInbound(response.event)
          : { prompt: buildInboundMessage(response.event, this.config.promptTag) };
        if (prepared.directAnswer?.trim()) {
          this.active.answer = prepared.directAnswer.trim();
          this.active.afterSend = prepared.afterSend;
          await this.flushAnswer();
          return;
        }
        const prompt = prepared.prompt;
        if (!validPreparedPrompt(prompt)) throw new Error("prepared Matrix prompt is empty");
        this.active.prompt = prompt;
        this.deps.inject(prompt);
        this.deps.log("info", `${this.config.laneLabel}: injected one Matrix turn`);
      } catch {
        this.active.answer = "Inbound preparation failed closed. No agent turn was started; please check the integration and retry.";
        await this.flushAnswer();
      }
    } catch {
      this.noteSidecarUnavailable();
    } finally {
      this.polling = false;
    }
  }

  captureAgentEnd(messages: unknown[]): void {
    if (!this.active || this.active.answer) return;
    const answer = extractFinalAssistantText(messages);
    if (!answer) return;
    if (answer.trim() === RETRYABLE_LLM_ERROR) {
      const retryCount = (this.active.modelRetryCount || 0) + 1;
      this.active.modelRetryCount = retryCount;
      this.active.modelRetryPending = true;
      this.active.modelNextRetryAt = Date.now() + modelRetryDelayMs(retryCount);
      return;
    }
    this.active.answer = answer;
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

  private async retryModelTurnIfReady(): Promise<void> {
    const active = this.active;
    if (!active?.modelRetryPending || !active.prompt || !this.safeIsIdle()) return;
    if (Date.now() < (active.modelNextRetryAt || 0)) return;
    active.modelRetryPending = false;
    this.deps.inject(active.prompt);
  }

  private async flushAnswer(): Promise<void> {
    const active = this.active;
    if (!active?.answer) return;
    const now = Date.now();
    if (now < this.answerNextAttemptAt) return;
    try {
      const response = await this.deps.ipc({
        op: "send",
        event_id: active.event.event_id,
        idempotency_key: `${this.config.idempotencyPrefix}:${active.event.event_id}`,
        body: active.answer,
      });
      this.markSidecarAvailable();
      if (!response.ok) {
        const reason = response.error?.trim() || response.status?.trim() || "send rejected";
        throw new Error(reason);
      }
      await this.stopActivity(active, "done");
      const afterSend = active.afterSend;
      this.active = undefined;
      this.markAnswerRetryRecovered();
      this.deps.log("info", `${this.config.laneLabel}: delivered one Matrix answer`);
      if (afterSend) {
        try {
          await afterSend();
        } catch {
          this.deps.log("warn", `${this.config.laneLabel}: post-send action failed`);
        }
      }
    } catch (error) {
      this.noteAnswerRetryPending(error);
      this.answerNextAttemptAt = Date.now() + this.computeRetryDelayMs();
    }
  }

  private async releaseActive(): Promise<void> {
    const active = this.active;
    this.active = undefined;
    this.resetAnswerRetryState();
    if (!active) return;
    await this.stopActivity(active, "stopped");
    try {
      await this.deps.ipc({ op: "release", event_id: active.event.event_id });
    } catch {
      // sidecar requeues claimed rows at restart
    }
  }

  private async startActivity(active: ActiveTurn): Promise<void> {
    try {
      const response = await this.deps.ipc({
        op: "activity_start",
        event_id: active.event.event_id,
      });
      if (response.ok && response.matrix_event_id) {
        active.activityStatusEventId = response.matrix_event_id;
      }
    } catch {
      this.deps.log("warn", `${this.config.laneLabel}: Matrix activity start unavailable`);
    }
  }

  private async refreshActivity(active: ActiveTurn): Promise<void> {
    if (!active.activityStatusEventId) {
      await this.startActivity(active);
      return;
    }
    const longRunning = !active.activityLongRunning
      && Date.now() - active.activityStartedAt >= LONG_RUNNING_ACTIVITY_MS;
    try {
      const response = await this.deps.ipc({
        op: "activity_heartbeat",
        event_id: active.event.event_id,
        status_event_id: active.activityStatusEventId,
        long_running: longRunning,
      });
      if (response.ok && longRunning) active.activityLongRunning = true;
    } catch {
      this.deps.log("warn", `${this.config.laneLabel}: Matrix activity heartbeat unavailable`);
    }
  }

  private async stopActivity(active: ActiveTurn, outcome: "done" | "stopped"): Promise<void> {
    try {
      await this.deps.ipc({
        op: "activity_stop",
        event_id: active.event.event_id,
        status_event_id: active.activityStatusEventId,
        outcome,
      });
    } catch {
      // Typing expires at the homeserver and progress is best-effort.
    }
  }

  private safeIsIdle(): boolean {
    try {
      return this.deps.isIdle();
    } catch {
      return false;
    }
  }

  private noteSidecarUnavailable(): void {
    const now = Date.now();
    if (!this.unavailableSince) {
      this.unavailableSince = now;
      this.lastUnavailableLogAt = 0;
    }
    if (now - this.lastUnavailableLogAt >= this.unavailableLogIntervalMs) {
      this.deps.log("warn", `${this.config.laneLabel}: sidecar is unavailable`);
      this.lastUnavailableLogAt = now;
    }
  }

  private markSidecarAvailable(): void {
    if (!this.unavailableSince) return;
    const downMs = Date.now() - this.unavailableSince;
    this.unavailableSince = undefined;
    this.lastUnavailableLogAt = 0;
    const downSec = Math.max(1, Math.round(downMs / 1000));
    this.deps.log("info", `${this.config.laneLabel}: sidecar recovered after ${downSec}s`);
  }

  private noteAnswerRetryPending(error: unknown): void {
    const now = Date.now();
    const reason = describeError(error);
    const reasonChanged = reason !== this.answerRetryReason;

    if (!this.answerRetrySince) {
      this.answerRetrySince = now;
      this.answerRetryCount = 1;
      this.answerRetryReason = reason;
      this.lastAnswerRetryLogAt = 0;
    } else {
      this.answerRetryCount += 1;
      this.answerRetryReason = reason;
    }

    if (
      reasonChanged
      || now - this.lastAnswerRetryLogAt >= this.answerRetryLogIntervalMs
      || this.answerRetryCount === 1
    ) {
      const ageSec = Math.max(1, Math.round((now - this.answerRetrySince) / 1000));
      this.deps.log(
        "warn",
        `${this.config.laneLabel}: answer remains queued for retry (${this.answerRetryReason}; attempts=${this.answerRetryCount}, age=${ageSec}s)`,
      );
      this.lastAnswerRetryLogAt = now;
    }
  }

  private markAnswerRetryRecovered(): void {
    if (!this.answerRetrySince) {
      this.resetAnswerRetryState();
      return;
    }
    const ageSec = Math.max(1, Math.round((Date.now() - this.answerRetrySince) / 1000));
    this.deps.log(
      "info",
      `${this.config.laneLabel}: queued answer delivered after ${this.answerRetryCount} attempts (${ageSec}s pending)`,
    );
    this.resetAnswerRetryState();
  }

  private resetAnswerRetryState(): void {
    this.answerRetrySince = undefined;
    this.answerRetryCount = 0;
    this.answerRetryReason = "send failed";
    this.answerNextAttemptAt = 0;
    this.lastAnswerRetryLogAt = 0;
  }

  private computeRetryDelayMs(): number {
    if (this.answerRetryCount <= 1) return 5000;
    const exponentialDelayMs = 5000 * 2 ** Math.min(6, this.answerRetryCount - 1);
    return Math.min(300000, exponentialDelayMs);
  }
}

function modelRetryDelayMs(retryCount: number): number {
  const exponentialDelayMs = 5000 * 2 ** Math.min(6, Math.max(0, retryCount - 1));
  return Math.min(300000, exponentialDelayMs);
}

function describeError(error: unknown): string {
  if (error instanceof Error && typeof error.message === "string" && error.message.trim()) {
    return error.message.trim();
  }
  return "send failed";
}

export function buildPrompt(body: string, kind: MatrixInboundEvent["kind"] = "text", promptTag = "matrix"): string {
  const trimmedTag = promptTag.trim() || "matrix";
  if (kind === "voice") return `[${trimmedTag} voice]\n${body.trim()}`;
  if (kind === "image") return `[${trimmedTag} image]\n${body.trim()}`;
  return `[${trimmedTag}]\n${body.trim()}`;
}

export function buildInboundMessage(event: MatrixInboundEvent, promptTag = "matrix"): MatrixUserMessage {
  if (event.kind !== "image" || !event.image) {
    return buildPrompt(event.body, event.kind, promptTag);
  }
  return [
    { type: "text", text: buildPrompt(event.body, "image", promptTag) },
    {
      type: "image",
      mimeType: event.image.media_type,
      data: event.image.data,
    },
  ];
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

function validPreparedPrompt(prompt: MatrixUserMessage | undefined): prompt is MatrixUserMessage {
  if (typeof prompt === "string") return prompt.trim().length > 0;
  return Array.isArray(prompt) && prompt.length > 0;
}

export function validInbound(event: MatrixInboundEvent): boolean {
  const common = typeof event.event_id === "string"
    && event.event_id.length > 0
    && typeof event.room_id === "string"
    && event.room_id.length > 0
    && typeof event.body === "string"
    && event.body.trim().length > 0
    && [...event.body].length <= 16000;
  if (!common) return false;
  if (event.kind === "text" || event.kind === "voice") return event.image === undefined;
  if (event.kind !== "image" || !event.image) return false;
  if (!["image/jpeg", "image/png", "image/gif", "image/webp"].includes(event.image.media_type)) {
    return false;
  }
  return validBoundedBase64(event.image.data);
}

function validBoundedBase64(data: unknown): boolean {
  if (typeof data !== "string" || data.length === 0 || data.length > 34_952_536) return false;
  if (data.length % 4 !== 0 || !/^[A-Za-z0-9+/]*={0,2}$/.test(data)) return false;
  const decoded = Buffer.from(data, "base64");
  if (decoded.length === 0 || decoded.length > 25 * 1024 * 1024) return false;
  return decoded.toString("base64") === data;
}

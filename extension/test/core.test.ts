import assert from "node:assert/strict";
import test from "node:test";

import {
  MatrixTransportController,
  buildPrompt,
  extractFinalAssistantText,
  loadConfig,
  type MatrixAgentTransportConfig,
} from "../src/core.ts";
import type { IpcRequest, IpcResponse } from "../src/ipc.ts";

const CONFIG: MatrixAgentTransportConfig = {
  enabled: true,
  socketPath: "/tmp/test.sock",
  pollMs: 1000,
  idempotencyPrefix: "test-reply",
  promptTag: "matrix test",
  laneLabel: "Test",
  topicEnabled: false,
  topicHelperPath: "",
};

test("activation and topic routing are exact and default off", () => {
  assert.equal(loadConfig({}).enabled, false);
  assert.equal(loadConfig({ PI_MATRIX_AGENT_ENABLED: "true" }).enabled, false);
  assert.equal(loadConfig({ PI_MATRIX_AGENT_ENABLED: "1" }).enabled, true);
  assert.equal(loadConfig({ PI_MATRIX_TOPIC_ENABLED: "true" }).topicEnabled, false);
  assert.equal(loadConfig({ PI_MATRIX_TOPIC_ENABLED: "1" }).topicEnabled, true);
});

test("prompt identifies text and voice with role-neutral tag", () => {
  assert.equal(buildPrompt(" hello ", "text", "matrix xo"), "[matrix xo]\nhello");
  assert.equal(buildPrompt(" spoken ", "voice", "matrix xo"), "[matrix xo voice]\nspoken");
});

test("extracts the last assistant text parts", () => {
  assert.equal(
    extractFinalAssistantText([
      { role: "user", content: [{ type: "text", text: "question" }] },
      { role: "assistant", content: [{ type: "thinking", text: "secret" }, { type: "text", text: "answer" }] },
    ]),
    "answer",
  );
});

test("prepares and captures before injecting a project-bound turn", async () => {
  const order: string[] = [];
  const calls: IpcRequest[] = [];
  const controller = new MatrixTransportController(CONFIG, {
    ipc: async (request): Promise<IpcResponse> => {
      calls.push(request);
      if (request.op === "claim") return { ok: true, event: { event_id: "$one", body: "hello", kind: "text" } };
      return { ok: true, status: "sent" };
    },
    isIdle: () => true,
    prepareInbound: async () => {
      order.push("capture");
      return { prompt: "prepared prompt" };
    },
    inject: (prompt) => order.push(`inject:${prompt}`),
    log: () => {},
  });

  await controller.tick();
  assert.deepEqual(order, ["capture", "inject:prepared prompt"]);
  controller.captureAgentEnd([{ role: "assistant", content: "answer" }]);
  await controller.onAgentSettled();
  assert.equal(controller.hasActiveTurn(), false);
  assert.deepEqual(calls.at(-1), {
    op: "send",
    event_id: "$one",
    idempotency_key: "test-reply:$one",
    body: "answer",
  });
});

test("topic commands send deterministic direct answers without model injection", async () => {
  const sends: IpcRequest[] = [];
  let injected = false;
  const controller = new MatrixTransportController(CONFIG, {
    ipc: async (request): Promise<IpcResponse> => {
      if (request.op === "claim") return { ok: true, event: { event_id: "$cmd", body: "/topic status", kind: "text" } };
      sends.push(request);
      return { ok: true, status: "sent" };
    },
    isIdle: () => true,
    prepareInbound: async () => ({ directAnswer: "Active: off." }),
    inject: () => { injected = true; },
    log: () => {},
  });

  await controller.tick();
  assert.equal(injected, false);
  assert.equal(controller.hasActiveTurn(), false);
  assert.deepEqual(sends, [{
    op: "send",
    event_id: "$cmd",
    idempotency_key: "test-reply:$cmd",
    body: "Active: off.",
  }]);
});

test("preparation failure fails closed without model injection", async () => {
  let injected = false;
  const sends: IpcRequest[] = [];
  const controller = new MatrixTransportController(CONFIG, {
    ipc: async (request): Promise<IpcResponse> => {
      if (request.op === "claim") return { ok: true, event: { event_id: "$bad", body: "hello", kind: "text" } };
      sends.push(request);
      return { ok: true, status: "sent" };
    },
    isIdle: () => true,
    prepareInbound: async () => { throw new Error("capture failed"); },
    inject: () => { injected = true; },
    log: () => {},
  });

  await controller.tick();
  assert.equal(injected, false);
  assert.equal((sends[0] as Extract<IpcRequest, { op: "send" }>).body.includes("failed closed"), true);
});

test("retryable provider error is never sent to Matrix and keeps the turn active", async () => {
  const sends: IpcRequest[] = [];
  const controller = new MatrixTransportController(CONFIG, {
    ipc: async (request): Promise<IpcResponse> => {
      if (request.op === "claim") return { ok: true, event: { event_id: "$retry", body: "hello", kind: "text" } };
      sends.push(request);
      return { ok: true, status: "sent" };
    },
    isIdle: () => true,
    inject: () => {},
    log: () => {},
  });

  await controller.tick();
  controller.captureAgentEnd([{ role: "assistant", content: "Error: Unknown error (no error details in response)" }]);
  await controller.onAgentSettled();
  assert.equal(controller.hasActiveTurn(), true);
  assert.deepEqual(sends, []);
});

test("session shutdown releases an unanswered claim", async () => {
  const calls: IpcRequest[] = [];
  const controller = new MatrixTransportController(CONFIG, {
    ipc: async (request) => {
      calls.push(request);
      if (request.op === "claim") return { ok: true, event: { event_id: "$one", body: "hello", kind: "text" } };
      return { ok: true, status: "released" };
    },
    isIdle: () => true,
    inject: () => {},
    log: () => {},
  });
  await controller.tick();
  await controller.onSessionShutdown();
  assert.deepEqual(calls, [{ op: "claim" }, { op: "release", event_id: "$one" }]);
});

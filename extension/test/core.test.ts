import assert from "node:assert/strict";
import test from "node:test";

import {
  MatrixTransportController,
  buildPrompt,
  extractFinalAssistantText,
  loadConfig,
  type MatrixAgentTransportConfig,
} from "../src/core.ts";
import type { MatrixIpcRequest, MatrixIpcResponse } from "../src/ipc.ts";

const CONFIG: MatrixAgentTransportConfig = {
  enabled: true,
  socketPath: "/tmp/test.sock",
  pollMs: 1000,
  idempotencyPrefix: "test-reply",
  promptTag: "matrix test",
  laneLabel: "Test",
};

test("activation is exact and default off", () => {
  assert.equal(loadConfig({}).enabled, false);
  assert.equal(loadConfig({ PI_MATRIX_AGENT_ENABLED: "true" }).enabled, false);
  assert.throws(
    () => loadConfig({ PI_MATRIX_AGENT_ENABLED: "1" }),
    /PI_MATRIX_AGENT_SOCKET is required/,
  );
  assert.equal(loadConfig({
    PI_MATRIX_AGENT_ENABLED: "1",
    PI_MATRIX_AGENT_SOCKET: "/private/runtime/pi-matrix-agent.sock",
  }).enabled, true);
});

test("prompt identifies text and voice with a configurable tag", () => {
  assert.equal(buildPrompt(" hello ", "text", "matrix agent"), "[matrix agent]\nhello");
  assert.equal(buildPrompt(" spoken ", "voice", "matrix agent"), "[matrix agent voice]\nspoken");
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

test("prepares inbound content before injecting a turn", async () => {
  const order: string[] = [];
  const calls: MatrixIpcRequest[] = [];
  const controller = new MatrixTransportController(CONFIG, {
    ipc: async (request): Promise<MatrixIpcResponse> => {
      calls.push(request);
      if (request.op === "claim") return { ok: true, event: { event_id: "$one", room_id: "!room:test", body: "hello", kind: "text" } };
      if (request.op === "activity_start") return { ok: true, status: "activity_started", matrix_event_id: "$activity" };
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
  assert.deepEqual(calls, [
    { op: "claim" },
    { op: "activity_start", event_id: "$one" },
    {
      op: "send",
      event_id: "$one",
      idempotency_key: "test-reply:$one",
      body: "answer",
    },
    { op: "activity_stop", event_id: "$one", status_event_id: "$activity", outcome: "done" },
  ]);
});

test("prepared direct answers bypass model injection", async () => {
  const sends: MatrixIpcRequest[] = [];
  let injected = false;
  const controller = new MatrixTransportController(CONFIG, {
    ipc: async (request): Promise<MatrixIpcResponse> => {
      if (request.op === "claim") return { ok: true, event: { event_id: "$cmd", room_id: "!room:test", body: "/topic status", kind: "text" } };
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
  assert.deepEqual(sends.filter((request) => request.op === "send"), [{
    op: "send",
    event_id: "$cmd",
    idempotency_key: "test-reply:$cmd",
    body: "Active: off.",
  }]);
});

test("preparation failure fails closed without model injection", async () => {
  let injected = false;
  const sends: MatrixIpcRequest[] = [];
  const controller = new MatrixTransportController(CONFIG, {
    ipc: async (request): Promise<MatrixIpcResponse> => {
      if (request.op === "claim") return { ok: true, event: { event_id: "$bad", room_id: "!room:test", body: "hello", kind: "text" } };
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
  const answer = sends.find((request): request is Extract<MatrixIpcRequest, { op: "send" }> => request.op === "send");
  assert.equal(answer?.body.includes("failed closed"), true);
});

test("retryable provider error is never sent to Matrix and keeps the turn active", async () => {
  const sends: MatrixIpcRequest[] = [];
  const controller = new MatrixTransportController(CONFIG, {
    ipc: async (request): Promise<MatrixIpcResponse> => {
      if (request.op === "claim") return { ok: true, event: { event_id: "$retry", room_id: "!room:test", body: "hello", kind: "text" } };
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
  assert.deepEqual(sends.filter((request) => request.op === "send"), []);
});

test("long-running turns refresh typing and update one sanitized notice", async () => {
  const originalNow = Date.now;
  let now = 1000;
  Date.now = () => now;
  try {
    const calls: MatrixIpcRequest[] = [];
    const controller = new MatrixTransportController(CONFIG, {
      ipc: async (request) => {
        calls.push(request);
        if (request.op === "claim") return { ok: true, event: { event_id: "$long", room_id: "!room:test", body: "hello", kind: "text" } };
        if (request.op === "activity_start") return { ok: true, status: "activity_started", matrix_event_id: "$activity" };
        return { ok: true, status: "activity_refreshed" };
      },
      isIdle: () => true,
      inject: () => {},
      log: () => {},
    });

    await controller.tick();
    now = 7000;
    await controller.tick();
    await controller.tick();

    assert.deepEqual(calls.filter((request) => request.op === "activity_heartbeat"), [
      {
        op: "activity_heartbeat",
        event_id: "$long",
        status_event_id: "$activity",
        long_running: true,
      },
      {
        op: "activity_heartbeat",
        event_id: "$long",
        status_event_id: "$activity",
        long_running: false,
      },
    ]);
  } finally {
    Date.now = originalNow;
  }
});

test("session shutdown releases an unanswered claim", async () => {
  const calls: MatrixIpcRequest[] = [];
  const controller = new MatrixTransportController(CONFIG, {
    ipc: async (request) => {
      calls.push(request);
      if (request.op === "claim") return { ok: true, event: { event_id: "$one", room_id: "!room:test", body: "hello", kind: "text" } };
      if (request.op === "activity_start") return { ok: true, status: "activity_started", matrix_event_id: "$activity" };
      return { ok: true, status: "released" };
    },
    isIdle: () => true,
    inject: () => {},
    log: () => {},
  });
  await controller.tick();
  await controller.onSessionShutdown();
  assert.deepEqual(calls, [
    { op: "claim" },
    { op: "activity_start", event_id: "$one" },
    { op: "activity_stop", event_id: "$one", status_event_id: "$activity", outcome: "stopped" },
    { op: "release", event_id: "$one" },
  ]);
});

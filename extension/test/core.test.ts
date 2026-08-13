import assert from "node:assert/strict";
import test from "node:test";

import {
  MatrixTransportController,
  buildPrompt,
  extractFinalAssistantText,
  loadConfig,
} from "../src/core.ts";
import type { IpcRequest, IpcResponse } from "../src/ipc.ts";

test("activation is exact and default off", () => {
  assert.equal(loadConfig({}).enabled, false);
  assert.equal(loadConfig({ PI_MATRIX_XO_ENABLED: "true" }).enabled, false);
  assert.equal(loadConfig({ PI_MATRIX_XO_ENABLED: "1" }).enabled, true);
});

test("prompt preserves only the transport marker and body", () => {
  assert.equal(buildPrompt("  hello XO  "), "[matrix]\nhello XO");
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

test("claims once, injects once, and sends after settled", async () => {
  const calls: IpcRequest[] = [];
  const injected: string[] = [];
  const ipc = async (request: IpcRequest): Promise<IpcResponse> => {
    calls.push(request);
    if (request.op === "claim") {
      return { ok: true, event: { event_id: "$one", body: "hello" } };
    }
    return { ok: true, status: "sent", matrix_event_id: "$reply" };
  };
  const controller = new MatrixTransportController({
    ipc,
    isIdle: () => true,
    inject: (prompt) => injected.push(prompt),
    log: () => {},
  });

  await controller.tick();
  await controller.tick();
  assert.deepEqual(injected, ["[matrix]\nhello"]);
  controller.captureAgentEnd([{ role: "assistant", content: [{ type: "text", text: "XO answer" }] }]);
  await controller.onAgentSettled();

  assert.equal(controller.hasActiveTurn(), false);
  assert.deepEqual(calls, [
    { op: "claim" },
    { op: "send", event_id: "$one", idempotency_key: "xo-reply:$one", body: "XO answer" },
  ]);
});

test("session shutdown releases an unanswered claim", async () => {
  const calls: IpcRequest[] = [];
  const controller = new MatrixTransportController({
    ipc: async (request) => {
      calls.push(request);
      if (request.op === "claim") return { ok: true, event: { event_id: "$one", body: "hello" } };
      return { ok: true, status: "released" };
    },
    isIdle: () => true,
    inject: () => {},
    log: () => {},
  });

  await controller.tick();
  await controller.onSessionShutdown();
  assert.equal(controller.hasActiveTurn(), false);
  assert.deepEqual(calls, [{ op: "claim" }, { op: "release", event_id: "$one" }]);
});

test("failed send stays queued and retries with the same idempotency key", async () => {
  const sends: IpcRequest[] = [];
  let sendAttempts = 0;
  const controller = new MatrixTransportController({
    ipc: async (request) => {
      if (request.op === "claim") return { ok: true, event: { event_id: "$one", body: "hello" } };
      if (request.op === "send") {
        sends.push(request);
        sendAttempts += 1;
        if (sendAttempts === 1) throw new Error("offline");
        return { ok: true, status: "sent" };
      }
      return { ok: true };
    },
    isIdle: () => true,
    inject: () => {},
    log: () => {},
  });

  await controller.tick();
  controller.captureAgentEnd([{ role: "assistant", content: "answer" }]);
  await controller.onAgentSettled();
  assert.equal(controller.hasActiveTurn(), true);
  await controller.tick();
  assert.equal(controller.hasActiveTurn(), false);
  assert.deepEqual(sends[0], sends[1]);
});

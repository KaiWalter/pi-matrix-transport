import net from "node:net";

export type InboundEvent = {
  event_id: string;
  body: string;
  kind: "text" | "voice";
};

export type IpcResponse = {
  ok: boolean;
  status?: string;
  event?: InboundEvent;
  error?: string;
  matrix_event_id?: string;
};

export type IpcRequest =
  | { op: "status" }
  | { op: "claim" }
  | { op: "release"; event_id: string }
  | { op: "send"; event_id: string; idempotency_key: string; body: string };

export async function request(
  socketPath: string,
  payload: IpcRequest,
  timeoutMs = 5_000,
): Promise<IpcResponse> {
  return await new Promise<IpcResponse>((resolve, reject) => {
    const socket = net.createConnection({ path: socketPath });
    let settled = false;
    let data = "";
    const timer = setTimeout(() => finish(new Error("Matrix sidecar IPC timeout")), timeoutMs);

    function finish(error?: Error, response?: IpcResponse): void {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      if (error) reject(error);
      else resolve(response!);
    }

    socket.setEncoding("utf8");
    socket.on("connect", () => socket.end(`${JSON.stringify(payload)}\n`));
    socket.on("data", (chunk) => {
      data += chunk;
      if (data.length > 65_536) finish(new Error("Matrix sidecar IPC response too large"));
    });
    socket.on("end", () => {
      try {
        const response = JSON.parse(data.trim()) as IpcResponse;
        if (!response || typeof response.ok !== "boolean") {
          finish(new Error("Invalid Matrix sidecar IPC response"));
          return;
        }
        finish(undefined, response);
      } catch {
        finish(new Error("Invalid Matrix sidecar IPC response"));
      }
    });
    socket.on("error", (error) => finish(error));
  });
}

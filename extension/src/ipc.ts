import net from "node:net";

export type MatrixInboundEvent = {
  event_id: string;
  room_id: string;
  body: string;
  kind: "text" | "voice";
};

export type MatrixProjectRoomBinding = {
  project_slug: string;
  room_id: string;
};

export type MatrixIpcResponse = {
  ok: boolean;
  status?: string;
  event?: MatrixInboundEvent;
  error?: string;
  matrix_event_id?: string;
  room_id?: string;
  project_rooms?: MatrixProjectRoomBinding[];
};

export type MatrixIpcRequest =
  | { op: "status" }
  | { op: "claim"; room_id?: string }
  | { op: "activity_start"; event_id: string }
  | { op: "activity_heartbeat"; event_id: string; status_event_id?: string; long_running: boolean }
  | { op: "activity_stop"; event_id: string; status_event_id?: string; outcome: "done" | "stopped" }
  | { op: "release"; event_id: string }
  | { op: "send"; event_id: string; idempotency_key: string; body: string }
  | { op: "project_room_add"; project_slug: string; display_name?: string }
  | { op: "project_room_remove"; project_slug: string }
  | { op: "project_room_list" };

export async function request(
  socketPath: string,
  payload: MatrixIpcRequest,
  timeoutMs = 5000,
): Promise<MatrixIpcResponse> {
  return await new Promise<MatrixIpcResponse>((resolve, reject) => {
    const socket = net.createConnection({ path: socketPath });
    let settled = false;
    let data = "";
    const timer = setTimeout(() => finish(new Error("Matrix sidecar IPC timeout")), timeoutMs);

    function finish(error?: Error, response?: MatrixIpcResponse): void {
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
      if (data.length > 65536) finish(new Error("Matrix sidecar IPC response too large"));
    });
    socket.on("end", () => {
      try {
        const response = JSON.parse(data.trim()) as MatrixIpcResponse;
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

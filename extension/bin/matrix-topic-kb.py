#!/usr/bin/env python3
"""Deterministic Matrix topic bindings backed by OpenKnowledge MCP."""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

KB_ROOT = Path(os.environ.get("PI_MATRIX_TOPIC_KB_ROOT", "/home/kai/knowledgebase"))
STATE_PATH = Path(
    os.environ.get(
        "PI_MATRIX_TOPIC_STATE",
        "/home/kai/.local/share/pi-matrix-transport/topics/xo.json",
    )
)
PROJECT_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
TOPIC_RE = re.compile(r"^#[a-z0-9][a-z0-9-]*$")
TOPIC_IN_TEXT_RE = re.compile(r"(?<![\w-])#[a-z0-9][a-z0-9-]*", re.IGNORECASE)
MAX_CONTEXT_CHARS = 12_000
MAX_CAPTURE_EVENTS = 1_000
CAPTURE_NAME = "matrix-captures.md"


class TopicError(RuntimeError):
    pass


class OkMcp:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.proc: subprocess.Popen[str] | None = None
        self.next_id = 1

    def __enter__(self) -> "OkMcp":
        port = self._server_port()
        cmd = ["ok", "--cwd", str(self.root), "--log-level", "silent", "mcp"]
        if port:
            cmd.extend(["--port", str(port)])
        else:
            cmd.append("--no-bundle-proxy")
        self.proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        response = self._request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "matrix-topic-kb", "version": "1.0"},
            },
        )
        if "error" in response:
            raise TopicError("OpenKnowledge initialization failed")
        self._notify("notifications/initialized", {})
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if not self.proc:
            return
        self.proc.terminate()
        try:
            self.proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            self.proc.kill()

    def exec(self, command: str) -> str:
        result = self._tool("exec", {"cwd": str(self.root), "command": command})
        structured = result.get("structuredContent")
        if isinstance(structured, dict) and isinstance(structured.get("text"), str):
            return structured["text"]
        for item in result.get("content", []):
            if isinstance(item, dict) and item.get("type") == "text":
                return str(item.get("text", ""))
        raise TopicError("OpenKnowledge returned no text")

    def write(self, path: str, content: str, summary: str) -> None:
        self._tool(
            "write",
            {
                "cwd": str(self.root),
                "document": {"path": path, "content": content, "position": "replace"},
                "summary": summary[:80],
            },
        )

    def _tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        response = self._request("tools/call", {"name": name, "arguments": arguments})
        if "error" in response:
            raise TopicError(f"OpenKnowledge {name} failed")
        result = response.get("result")
        if not isinstance(result, dict) or result.get("isError"):
            raise TopicError(f"OpenKnowledge {name} rejected request")
        return result

    def _server_port(self) -> int | None:
        proc = subprocess.run(
            ["ok", "--cwd", str(self.root), "status", "--json"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode != 0:
            return None
        try:
            value = json.loads(proc.stdout).get("server", {}).get("port")
            return int(value) if value else None
        except (ValueError, TypeError, json.JSONDecodeError):
            return None

    def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        assert self.proc and self.proc.stdin and self.proc.stdout
        request_id = self.next_id
        self.next_id += 1
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}) + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        if not line:
            raise TopicError("OpenKnowledge MCP closed unexpectedly")
        return json.loads(line)

    def _notify(self, method: str, params: dict[str, Any]) -> None:
        assert self.proc and self.proc.stdin
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "method": method, "params": params}) + "\n")
        self.proc.stdin.flush()


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_topic(value: str) -> str:
    topic = value.strip().lower()
    if not topic.startswith("#"):
        topic = f"#{topic}"
    if not TOPIC_RE.fullmatch(topic):
        raise TopicError("topic must use #lowercase-kebab-case")
    return topic


def project_path(slug: str) -> str:
    slug = slug.strip().lower()
    if not PROJECT_RE.fullmatch(slug):
        raise TopicError("project slug must use lowercase-kebab-case")
    return f"projects/{slug}"


def default_state() -> dict[str, Any]:
    return {"version": 1, "bindings": {}, "activeTopic": None, "capturedEventIds": [], "updatedAt": utc_now()}


def read_state() -> dict[str, Any]:
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or not isinstance(data.get("bindings"), dict):
            return default_state()
        data.setdefault("activeTopic", None)
        data.setdefault("capturedEventIds", [])
        return data
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default_state()


def write_state(state: dict[str, Any]) -> None:
    state["updatedAt"] = utc_now()
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temp_name = tempfile.mkstemp(prefix="topics-", suffix=".json", dir=STATE_PATH.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temp_name, STATE_PATH)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def clean_ok_cat(raw: str, path: str) -> str:
    header = f"==> {path} <==\n"
    if raw.startswith(header):
        raw = raw[len(header):]
    marker = "\n### Referenced files\n"
    if marker in raw:
        raw = raw.split(marker, 1)[0]
    return raw.strip()


def read_project_context(ok: OkMcp, project: str) -> str:
    index_path = f"{project}/index.md"
    raw = ok.exec(f"cat {index_path}")
    content = clean_ok_cat(raw, index_path)
    if not content:
        raise TopicError("project index is empty")
    return content[:MAX_CONTEXT_CHARS]


def capture_document(project: str, topic: str, body: str, existing: str | None) -> str:
    if existing is None:
        content = (
            "---\n"
            "title: Matrix Topic Captures\n"
            "description: User turns captured from a bound Matrix project topic.\n"
            "tags: [matrix, capture]\n"
            "origin: generated\n"
            "type: matrix-captures\n"
            "---\n\n"
            "# Matrix Topic Captures\n"
        )
    else:
        content = existing.rstrip()
    quoted = "\n".join(f"> {line}" if line else ">" for line in body.strip().splitlines())
    return f"{content}\n\n## {utc_now()} — {topic}\n\n{quoted}\n"


def append_capture(ok: OkMcp, project: str, topic: str, body: str) -> None:
    path = f"{project}/generated/{CAPTURE_NAME}"
    try:
        existing = clean_ok_cat(ok.exec(f"cat {path}"), path)
    except TopicError:
        existing = None
    ok.write(path, capture_document(project, topic, body, existing), f"Capture Matrix turn for {topic}")


def command_result(body: str, state: dict[str, Any], ok: OkMcp) -> dict[str, Any] | None:
    parts = body.strip().split()
    if not parts or parts[0].lower() != "/topic":
        return None
    if len(parts) == 1:
        return {"directAnswer": "Use /topic onboard, use, status, off, or remove."}
    action = parts[1].lower()
    bindings: dict[str, str] = state["bindings"]

    if action == "onboard":
        if len(parts) < 4:
            raise TopicError("usage: /topic onboard <project-slug> <#topic> [<#alias> ...]")
        project = project_path(parts[2])
        read_project_context(ok, project)
        topics = [normalize_topic(value) for value in parts[3:]]
        collisions = [topic for topic in topics if topic in bindings and bindings[topic] != project]
        if collisions:
            raise TopicError(f"topic already bound: {', '.join(collisions)}")
        for topic in topics:
            bindings[topic] = project
        state["activeTopic"] = topics[0]
        write_state(state)
        return {"directAnswer": f"Onboarded {', '.join(topics)} to {project}; active topic is {topics[0]}."}

    if action == "use":
        if len(parts) != 3:
            raise TopicError("usage: /topic use <#topic>")
        topic = normalize_topic(parts[2])
        if topic not in bindings:
            raise TopicError(f"topic is not onboarded: {topic}")
        state["activeTopic"] = topic
        write_state(state)
        return {"directAnswer": f"Active Matrix topic: {topic} -> {bindings[topic]}."}

    if action == "status":
        active = state.get("activeTopic")
        mapping = ", ".join(f"{key}->{value}" for key, value in sorted(bindings.items())) or "none"
        active_text = f"{active}->{bindings.get(active)}" if active in bindings else "off"
        return {"directAnswer": f"Active: {active_text}. Bindings: {mapping}."}

    if action == "off":
        state["activeTopic"] = None
        write_state(state)
        return {"directAnswer": "Matrix project topic capture is off."}

    if action == "remove":
        if len(parts) != 3:
            raise TopicError("usage: /topic remove <#topic>")
        topic = normalize_topic(parts[2])
        if topic not in bindings:
            raise TopicError(f"topic is not onboarded: {topic}")
        del bindings[topic]
        if state.get("activeTopic") == topic:
            state["activeTopic"] = None
        write_state(state)
        return {"directAnswer": f"Removed Matrix topic binding {topic}."}

    raise TopicError(f"unknown /topic action: {action}")


def prepare(body: str, event_id: str) -> dict[str, Any]:
    state = read_state()
    with OkMcp(KB_ROOT) as ok:
        command = command_result(body, state, ok)
        if command is not None:
            return command

        explicit = {normalize_topic(value) for value in TOPIC_IN_TEXT_RE.findall(body)}
        mapped = {topic: state["bindings"][topic] for topic in explicit if topic in state["bindings"]}
        projects = set(mapped.values())
        if len(projects) > 1:
            raise TopicError("message contains topics bound to different projects")
        if projects:
            selected_project = next(iter(projects))
            selected_topic = sorted(topic for topic, project in mapped.items() if project == selected_project)[0]
            state["activeTopic"] = selected_topic
            write_state(state)
        else:
            selected_topic = state.get("activeTopic")
            selected_project = state["bindings"].get(selected_topic) if selected_topic else None

        if not selected_topic or not selected_project:
            return {"prompt": body.strip(), "topic": None, "project": None, "captured": False}

        context = read_project_context(ok, selected_project)
        captured_ids = [str(value) for value in state.get("capturedEventIds", [])]
        if event_id not in captured_ids:
            append_capture(ok, selected_project, selected_topic, body)
            captured_ids.append(event_id)
            state["capturedEventIds"] = captured_ids[-MAX_CAPTURE_EVENTS:]
            write_state(state)

        prompt = (
            f"[matrix project topic]\n"
            f"Active topic: {selected_topic}\n"
            f"Bound OpenKnowledge project: {selected_project}\n"
            "The user turn has already been captured into this project. Use the bounded project context below.\n\n"
            f"<project_context>\n{context}\n</project_context>\n\n"
            f"<user_message>\n{body.strip()}\n</user_message>"
        )
        return {"prompt": prompt, "topic": selected_topic, "project": selected_project, "captured": True}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["prepare"])
    parser.add_argument("--event-id", required=True)
    args = parser.parse_args()
    body = sys.stdin.read()
    if not body.strip():
        print(json.dumps({"ok": False, "error": "empty message"}))
        return 2
    try:
        result = prepare(body, args.event_id)
        print(json.dumps({"ok": True, **result}, ensure_ascii=False))
        return 0
    except TopicError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

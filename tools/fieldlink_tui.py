#!/usr/bin/env python3
"""Interactive two-radio FieldLink test runner using only the standard library."""

from __future__ import annotations

import argparse
import curses
import curses.textpad
import json
import os
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO


REPOSITORY = Path(__file__).resolve().parents[1]
CLI = ("npm", "run", "--silent", "fieldlink", "--")
RESULTS_TRANSCRIPT = REPOSITORY / "tools" / "results.txt"
DISCOVERY_TIMEOUT_SECONDS = 30
STOP_TIMEOUT_SECONDS = 30
MAX_TIMEOUT_MS = 24 * 60 * 60 * 1000
MAX_RESOURCE_LIST_LIMIT = 1000


class Cancelled(Exception):
    pass


@dataclass(frozen=True)
class PayloadPreset:
    payload_bytes: int
    encoded_bytes: int
    delivery: str
    fragments: int

    @property
    def label(self) -> str:
        if self.delivery == "complete":
            detail = "single frame"
        else:
            detail = f"{self.fragments} fragments"
        return f"{self.payload_bytes:,} bytes  {detail}"


@dataclass(frozen=True)
class MessageChoice:
    message_id: int
    name: str
    priority: str
    default_payload_bytes: int
    maximum_payload_bytes: int
    presets: tuple[PayloadPreset, ...]

    @property
    def label(self) -> str:
        return f"{self.name}  ID {self.message_id}, {self.priority} priority"


@dataclass(frozen=True)
class RadioChoice:
    path: str
    manufacturer: str | None
    serial_number: str | None

    @property
    def label(self) -> str:
        details = [value for value in (self.manufacturer, self.serial_number) if value]
        identity = self.path if not details else f"{self.path}  {' | '.join(details)}"
        return f"{identity}  unverified"


@dataclass(frozen=True)
class RunConfiguration:
    message: MessageChoice
    radio_a: RadioChoice
    radio_b: RadioChoice
    payload_bytes: int | None
    resource_request: dict[str, Any] | None
    resource_request_path: Path | None
    retry_strategy: str
    timeout_ms: int
    output: Path


class RunTranscript:
    """Write the current TUI run as one continuously flushed text transcript."""

    def __init__(self, path: Path, config: RunConfiguration):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = path.open("w", encoding="utf-8")
        self._finished = False
        self._write("SENT MESSAGE\n============\n")
        if config.resource_request is not None:
            self._write(
                json.dumps(config.resource_request, indent=2, ensure_ascii=False)
                + "\n"
            )
        else:
            self._write(
                f"Test exercise input: {config.payload_bytes:,} payload bytes\n"
                "The CLI generates the Test correlation ID and deterministic payload.\n"
            )
        self._write("\nRUN LOG\n=======\n")

    def append(self, line: str | None) -> None:
        if not self._finished and line:
            self._write(line.rstrip() + "\n")

    def finish(
        self,
        received_message: dict[str, Any] | None = None,
        result_lines: list[str] | None = None,
    ) -> None:
        if self._finished:
            return
        self._finished = True
        if result_lines:
            self._write("\nRUN RESULT\n==========\n")
            for line in result_lines:
                self._write(line.rstrip() + "\n")
        self._write("\nRECEIVED MESSAGE\n================\n")
        if received_message is None:
            self._write("(no response received)\n")
        else:
            self._write(
                json.dumps(received_message, indent=2, ensure_ascii=False) + "\n"
            )
        self._handle.close()

    def _write(self, text: str) -> None:
        self._handle.write(text)
        self._handle.flush()


class FieldLinkCli:
    def run_json(self, *arguments: str) -> Any:
        try:
            completed = subprocess.run(
                [*CLI, *arguments],
                cwd=REPOSITORY,
                text=True,
                capture_output=True,
                check=False,
                timeout=DISCOVERY_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(
                f"FieldLink CLI did not respond within {DISCOVERY_TIMEOUT_SECONDS} seconds"
            ) from error
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise RuntimeError(detail or "FieldLink CLI failed")
        try:
            return json.loads(completed.stdout)
        except json.JSONDecodeError as error:
            raise RuntimeError("FieldLink CLI returned invalid JSON") from error

    def messages(self) -> tuple[list[MessageChoice], list[str]]:
        catalog = self.run_json("messages", "list", "--json")
        messages = []
        for raw in catalog.get("messages", []):
            exercise = raw["exercise"]
            presets = tuple(
                PayloadPreset(
                    payload_bytes=item["payloadBytes"],
                    encoded_bytes=item["encodedBytes"],
                    delivery=item["delivery"],
                    fragments=item["fragments"],
                )
                for item in exercise["presets"]
            )
            messages.append(
                MessageChoice(
                    message_id=raw["id"],
                    name=raw["name"],
                    priority=raw["defaultPriority"],
                    default_payload_bytes=exercise["defaultPayloadBytes"],
                    maximum_payload_bytes=exercise["maximumPayloadBytes"],
                    presets=presets,
                )
            )
        strategies = [item["name"] for item in catalog.get("retryStrategies", [])]
        return messages, strategies

    def radios(self) -> list[RadioChoice]:
        return [
            RadioChoice(
                path=item["path"],
                manufacturer=item.get("manufacturer"),
                serial_number=item.get("serialNumber"),
            )
            for item in self.run_json("radios", "list", "--json")
        ]

    def start_test(self, config: RunConfiguration) -> subprocess.Popen[str]:
        return subprocess.Popen(
            build_test_command(config),
            cwd=REPOSITORY,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            start_new_session=True,
        )


def build_test_command(config: RunConfiguration) -> list[str]:
    command = [
        *CLI,
        "test",
        "--message",
        config.message.name,
        "--a",
        config.radio_a.path,
        "--b",
        config.radio_b.path,
        "--retry-strategy",
        config.retry_strategy,
        "--timeout-ms",
        str(config.timeout_ms),
        "--output",
        str(config.output),
        "--allow-inbox-drain",
    ]
    if config.resource_request_path is not None:
        command.extend(["--resource-request", str(config.resource_request_path)])
    elif config.payload_bytes is not None:
        command.extend(["--payload-size", str(config.payload_bytes)])
    else:
        raise RuntimeError("Run configuration has no message input")
    return command


@dataclass
class RunView:
    logs: list[str] = field(default_factory=list)
    fragment_totals: dict[str, int] = field(default_factory=dict)
    frames_sent: dict[str, int] = field(default_factory=lambda: {"A": 0, "B": 0})
    frames_received: dict[str, int] = field(
        default_factory=lambda: {"A": 0, "B": 0}
    )
    fragments_sent: int = 0
    fragments_received: int = 0
    retransmissions: int = 0
    receipt_requests: int = 0
    receipts: int = 0
    snr_samples: list[float] = field(default_factory=list)
    selected_channel: int | None = None
    selected_channel_name: str | None = None
    received_message: dict[str, Any] | None = None
    resource_response: dict[str, Any] | None = None

    def add_output(self, source: str, line: str) -> str | None:
        clean = line.strip()
        if source == "diag" and clean.startswith("[adapter "):
            return None
        if clean:
            rendered = f"[{source}] {clean}"
            self.logs.append(rendered)
            return rendered
        return None

    def observe(self, record: dict[str, Any]) -> str:
        timestamp = str(record.get("at", ""))[11:19]
        kind = record.get("type")
        data = record.get("data", {})
        prefix = f"{timestamp} " if timestamp else ""
        if kind == "node-event":
            radio = str(data.get("radio", "?"))
            event = data.get("event", {})
            text = self._node_event(radio, event)
        elif kind == "channel-scan-started":
            text = "checking every available MeshCore channel slot on both radios"
        elif kind == "channel-scan":
            radio = str(data.get("radio", "?"))
            configured = [
                f"{channel.get('index', '?')}:{channel.get('name') or 'unnamed'}"
                for channel in data.get("channels", [])
                if channel.get("configured")
            ]
            text = f"[{radio}] configured channels {', '.join(configured) or 'none'}"
        elif kind == "channel-selected":
            channel = data.get("channel", {})
            index = channel.get("index")
            name = channel.get("name")
            self.selected_channel = int(index) if isinstance(index, int) else None
            self.selected_channel_name = str(name) if name else None
            detail = f" {self.selected_channel_name}" if self.selected_channel_name else ""
            text = f"using MeshCore channel {index}{detail}"
        elif kind == "ready":
            a = data.get("a", {})
            b = data.get("b", {})
            text = f"radios ready  A {a.get('nodeId', '?')}  B {b.get('nodeId', '?')}"
        elif kind == "message":
            radio = str(data.get("radio", "?"))
            message = data.get("message", {}).get("message", {})
            if isinstance(message, dict) and message.get("kind") == "response":
                self.received_message = message
                if message.get("type") == "resource" and radio == "A":
                    self.resource_response = message
            text = f"[{radio}] decoded {message.get('type', '?')} {message.get('kind', '')}".rstrip()
        elif kind == "resource-gateway-ready":
            text = f"[{data.get('radio', '?')}] Atlas Resource gateway ready for source {data.get('allowedSource', '?')}"
        elif kind == "exercise-complete":
            text = f"response correlated  {data.get('correlation', '?')}"
        elif kind == "test-failed":
            text = f"exercise failed  {data.get('message', 'unknown error')}"
        elif kind == "adapter-stderr":
            text = f"[{data.get('radio', '?')}] {str(data.get('message', '')).strip()}"
        elif kind == "inbox-message":
            text = f"[{data.get('radio', '?')}] consumed MeshCore inbox item"
        elif kind == "interrupted":
            text = f"interrupted by {data.get('signal', 'signal')}"
        elif kind == "cleanup-error":
            text = f"cleanup error  {data.get('message', 'unknown error')}"
        else:
            text = f"{kind}  {compact_json(data)}"
        rendered = prefix + text
        self.logs.append(rendered)
        if len(self.logs) > 5000:
            del self.logs[:1000]
        return rendered

    def _node_event(self, radio: str, event: dict[str, Any]) -> str:
        kind = str(event.get("type", "event"))
        logical_id = str(event.get("logicalId", ""))
        if kind == "frame-sent":
            self.frames_sent[radio] = self.frames_sent.get(radio, 0) + 1
            return f"[{radio}] sent {event.get('bytes', '?')}-byte {event.get('priority', '?')} frame"
        if kind == "frame-received":
            if "snrDb" in event:
                self.snr_samples.append(float(event["snrDb"]))
            self.frames_received[radio] = self.frames_received.get(radio, 0) + 1
            snr = f"  SNR {event['snrDb']:.1f} dB" if "snrDb" in event else ""
            return f"[{radio}] received {event.get('frameKind', '?')} frame, {event.get('bytes', '?')} bytes{snr}"
        if kind == "transfer-started":
            total = int(event.get("fragmentCount", 0))
            self.fragment_totals[logical_id] = total
            return f"[{radio}] chunking {event.get('encodedBytes', '?')} encoded bytes into {total} fragments"
        if kind == "transfer-accepted":
            total = int(event.get("fragmentCount", 0))
            self.fragment_totals[logical_id] = total
            return f"[{radio}] accepted transfer with {total} fragments"
        if kind in {"fragment-sent", "fragment-retransmitted", "fragment-received"}:
            index = int(event.get("fragmentIndex", 0)) + 1
            total = self.fragment_totals.get(logical_id, "?")
            if kind == "fragment-received":
                self.fragments_received += 1
                action = "received chunk"
            elif kind == "fragment-retransmitted":
                self.retransmissions += 1
                action = "resent unacknowledged chunk"
            else:
                self.fragments_sent += 1
                action = "sent chunk"
            return f"[{radio}] {action} {index}/{total}"
        if kind in {"receipt-sent", "receipt-received"}:
            self.receipts += 1
            action = "sent receipt" if kind == "receipt-sent" else "received receipt"
            return f"[{radio}] {action} bitmap {event.get('bitmap', '?')}"
        if kind == "receipt-request-sent":
            self.receipt_requests += 1
            start = int(event.get("windowStart", 0)) + 1
            count = int(event.get("windowCount", 0))
            return f"[{radio}] requested receipt for chunks {start}-{start + count - 1}"
        if kind == "message-received":
            return f"[{radio}] delivered {event.get('messageName', '?')} via {event.get('delivery', '?')}"
        if kind == "transfer-completed":
            return f"[{radio}] transfer complete, {event.get('retransmissions', 0)} retransmissions"
        if kind in {"protocol-error", "transport-error", "transfer-failed"}:
            return f"[{radio}] {kind}  {event.get('message', event.get('error', 'unknown error'))}"
        return f"[{radio}] {kind}  {compact_json(event)}"


def compact_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), default=str)[:240]


def summary_lines(
    summary: dict[str, Any] | None,
    view: RunView,
    config: RunConfiguration,
) -> list[str]:
    if summary is None:
        return [
            f"Frames sent A/B {view.frames_sent['A']}/{view.frames_sent['B']}   received A/B {view.frames_received['A']}/{view.frames_received['B']}",
            f"Chunks sent {view.fragments_sent}   received {view.fragments_received}   retransmitted {view.retransmissions}",
            f"Receipt requests {view.receipt_requests}   receipt events {view.receipts}",
        ]
    request = summary.get("request", {})
    response = summary.get("response", {})
    verification = summary.get("verification", {})
    elapsed_ms = float(summary.get("elapsedMs", 0.0))
    lines = [
        f"Status {summary.get('status', '?')}   condition {summary.get('condition', '?')}   elapsed {elapsed_ms:.2f} ms   correlation {verification.get('correlation', 'unconfirmed')}",
        f"Request {request.get('delivery', '?')}   encoded {request.get('encodedBytes', '?')} bytes   fragments {request.get('fragments', '?')}   retransmissions {request.get('retransmissions', '?')}",
        f"Sender duration {float(request.get('durationMs', 0.0)):.2f} ms   receipt requests {request.get('receiptRequests', 0)}   request retries {request.get('receiptRequestRetries', 0)}   receipts {request.get('receipts', 0)}",
        f"Response {response.get('delivery', '?')}   encoded {response.get('encodedBytes', '?')} bytes   fragments {response.get('fragments', '?')}   retransmissions {response.get('retransmissions', '?')}",
        f"Response duration {float(response.get('durationMs', 0.0)):.2f} ms   receipt requests {response.get('receiptRequests', 0)}   request retries {response.get('receiptRequestRetries', 0)}   receipts {response.get('receipts', 0)}   digest {verification.get('responseDigest', 'not applicable')}",
        f"Observed frames sent A/B {view.frames_sent['A']}/{view.frames_sent['B']}   received A/B {view.frames_received['A']}/{view.frames_received['B']}",
    ]
    selected_channel = summary.get("selectedChannel")
    if isinstance(selected_channel, dict) and "index" in selected_channel:
        name = selected_channel.get("name")
        detail = f" {name}" if name else ""
        lines.insert(1, f"MeshCore channel {selected_channel['index']}{detail}")
    if "atlasStatus" in verification:
        lines.append(f"Atlas status {verification['atlasStatus']}")
    if elapsed_ms > 0 and config.payload_bytes is not None:
        rate = config.payload_bytes / (elapsed_ms / 1000)
        lines.append(f"Request payload per echo round trip {format_rate(rate)}")
    if view.snr_samples:
        mean = sum(view.snr_samples) / len(view.snr_samples)
        lines.append(
            f"SNR {min(view.snr_samples):.1f}/{mean:.1f}/{max(view.snr_samples):.1f} dB min/mean/max across {len(view.snr_samples)} received frames"
        )
    if summary.get("error"):
        lines.append(f"Error {summary['error']}")
    diagnostic_errors = summary.get("diagnosticErrors", [])
    if diagnostic_errors:
        lines.append(f"Diagnostic errors {len(diagnostic_errors)}")
    lines.append(f"Artifacts {config.output}")
    return lines


def format_rate(bytes_per_second: float) -> str:
    if bytes_per_second >= 1024:
        return f"{bytes_per_second / 1024:.2f} KiB/s"
    return f"{bytes_per_second:.1f} B/s"


@dataclass
class ResourceDraft:
    operation: str = "get"
    resource_type: str = "entity"
    request_id: str = field(
        default_factory=lambda: f"fieldlink-{uuid.uuid4().hex[:12]}"
    )
    resource_id: str = "entity-fieldlink-demo"
    limit: int = 50
    cursor: str = ""
    body: dict[str, Any] = field(default_factory=dict)

    def message(self) -> dict[str, Any]:
        request: dict[str, Any] = {
            "type": "resource",
            "kind": "request",
            "operation": self.operation,
            "request_id": self.request_id,
            "resource_type": self.resource_type,
        }
        if self.operation in {"get", "patch", "delete"}:
            request["resource_id"] = self.resource_id
        if self.operation == "list":
            query: dict[str, Any] = {"limit": self.limit}
            if self.cursor:
                query["cursor"] = self.cursor
            request["query"] = query
        if self.operation in {"create", "patch"}:
            request["body"] = self.body
        return request


def resource_types(operation: str) -> tuple[str, ...]:
    if operation in {"get", "list"}:
        return ("entity", "object", "task")
    return ("entity", "object")


def default_resource_body(operation: str, resource_type: str) -> dict[str, Any]:
    if operation == "create" and resource_type == "entity":
        return {"entity_id": "entity-fieldlink-demo", "entity_type": "asset"}
    if operation == "create":
        return {"object_id": "object-fieldlink-demo", "type": "application/json"}
    if resource_type == "entity":
        return {"alias": "FieldLink update"}
    return {"type": "application/json"}


def resource_fields(draft: ResourceDraft) -> list[tuple[str, str, str]]:
    fields = [
        ("operation", "Operation", draft.operation),
        ("resource_type", "Resource", draft.resource_type),
        ("request_id", "Request ID", draft.request_id),
    ]
    if draft.operation in {"get", "patch", "delete"}:
        fields.append(("resource_id", "Resource ID", draft.resource_id))
    if draft.operation == "list":
        fields.extend(
            [
                ("limit", "Limit", str(draft.limit)),
                ("cursor", "Cursor", draft.cursor or "(none)"),
            ]
        )
    if draft.operation in {"create", "patch"}:
        count = len(draft.body)
        body_summary = f"{count} field" if count == 1 else f"{count} fields"
        fields.append(("body", "Body JSON", body_summary))
    return fields


def edit_resource_request(screen: curses.window) -> dict[str, Any]:
    draft = ResourceDraft()
    selected = 0
    while True:
        fields = resource_fields(draft)
        selected %= len(fields)
        draw_resource_editor(screen, draft, fields, selected)
        key = screen.getch()
        if key in (ord("q"), 27):
            raise Cancelled()
        if key == curses.KEY_UP:
            selected = (selected - 1) % len(fields)
        elif key == curses.KEY_DOWN:
            selected = (selected + 1) % len(fields)
        elif key in (ord("s"), ord("S")):
            return draft.message()
        elif key in (10, 13, curses.KEY_ENTER):
            edit_resource_field(screen, draft, fields[selected][0])


def draw_resource_editor(
    screen: curses.window,
    draft: ResourceDraft,
    fields: list[tuple[str, str, str]],
    selected: int,
) -> None:
    screen.erase()
    height, width = screen.getmaxyx()
    put(screen, 0, 0, "FieldLink Resource request", curses.A_BOLD)
    put(screen, 1, 0, "Edit fields and inspect the exact JSON sent over FieldLink.", curses.A_DIM)
    if width < 72 or height < 14:
        put(screen, 4, 0, "Resize the terminal to at least 72 x 14.", curses.A_BOLD)
        put(screen, height - 1, 0, "q back", curses.A_DIM)
        screen.refresh()
        return
    divider = max(31, min(width // 2, 48))
    separator = " | "
    heading = f"{'Resource request':<{divider}}{separator}Generated JSON"
    put(screen, 3, 0, heading[: width - 1], curses.A_BOLD)
    preview = json.dumps(draft.message(), indent=2, ensure_ascii=False).splitlines()
    visible_rows = min(max(len(fields), len(preview)), max(0, height - 6))
    for index in range(visible_rows):
        if index < len(fields):
            _key, label, value = fields[index]
            marker = ">" if index == selected else " "
            left = f"{marker} {label:<12} {value}"[:divider]
        else:
            left = ""
        right = preview[index] if index < len(preview) else ""
        line = f"{left:<{divider}}{separator}{right}"
        put(screen, index + 5, 0, line[: width - 1])
    put(
        screen,
        height - 1,
        0,
        "↑/↓ select   Enter edit   s continue   q back",
        curses.A_DIM,
    )
    screen.refresh()


def edit_resource_field(
    screen: curses.window, draft: ResourceDraft, field_name: str
) -> None:
    if field_name == "operation":
        operations = ["create", "get", "list", "patch", "delete"]
        draft.operation = operations[choose(screen, "Operation", operations)]
        if draft.resource_type not in resource_types(draft.operation):
            draft.resource_type = "entity"
        if draft.operation in {"create", "patch"}:
            draft.body = default_resource_body(draft.operation, draft.resource_type)
        return
    if field_name == "resource_type":
        choices = list(resource_types(draft.operation))
        draft.resource_type = choices[choose(screen, "Resource", choices)]
        draft.resource_id = f"{draft.resource_type}-fieldlink-demo"
        if draft.operation in {"create", "patch"}:
            draft.body = default_resource_body(draft.operation, draft.resource_type)
        return
    if field_name == "request_id":
        draft.request_id = read_text(
            screen, "Request ID", draft.request_id, allow_empty=False
        )
    elif field_name == "resource_id":
        draft.resource_id = read_text(
            screen, "Resource ID", draft.resource_id, allow_empty=False
        )
    elif field_name == "limit":
        draft.limit = read_integer(
            screen,
            "Maximum resources to return.",
            draft.limit,
            MAX_RESOURCE_LIST_LIMIT,
            minimum=1,
        )
    elif field_name == "cursor":
        draft.cursor = read_text(
            screen,
            "Cursor. Leave blank for the first page.",
            draft.cursor,
            clear_on_blank=True,
        )
    elif field_name == "body":
        draft.body = edit_json_object(screen, draft.body)


def read_text(
    screen: curses.window,
    prompt: str,
    default: str,
    *,
    allow_empty: bool = True,
    clear_on_blank: bool = False,
) -> str:
    while True:
        screen.erase()
        put(screen, 0, 0, "FieldLink Resource request", curses.A_BOLD)
        put(screen, 2, 0, prompt)
        put(screen, 4, 0, f"Value [{default}]: ")
        curses.echo()
        curses.curs_set(1)
        try:
            raw = screen.getstr(4, len(f"Value [{default}]: "), 1024).decode().strip()
        finally:
            curses.noecho()
            curses.curs_set(0)
        if raw:
            return raw
        if clear_on_blank and allow_empty:
            return ""
        if default or allow_empty:
            return default if default else ""
        put(screen, 6, 0, "A non-empty value is required.", curses.A_BOLD)
        screen.getch()


def edit_json_object(
    screen: curses.window, current: dict[str, Any]
) -> dict[str, Any]:
    while True:
        height, width = screen.getmaxyx()
        popup_height = max(8, height - 4)
        popup_width = max(40, width - 6)
        top = max(0, (height - popup_height) // 2)
        left = max(0, (width - popup_width) // 2)
        popup = curses.newwin(popup_height, popup_width, top, left)
        popup.erase()
        popup.box()
        put(popup, 0, 2, " Body JSON. Ctrl-G saves ", curses.A_BOLD)
        editor = popup.derwin(popup_height - 2, popup_width - 2, 1, 1)
        initial = json.dumps(current, indent=2, ensure_ascii=False)
        capacity = (popup_height - 2) * (popup_width - 3)
        if len(initial) > capacity:
            show_error(
                screen,
                "Body JSON does not fit this terminal. Resize before editing it.",
            )
            return current
        try:
            editor.addstr(initial)
        except curses.error:
            show_error(
                screen,
                "Body JSON does not fit this terminal. Resize before editing it.",
            )
            return current
        popup.noutrefresh()
        editor.noutrefresh()
        curses.doupdate()
        curses.curs_set(1)
        try:
            raw = curses.textpad.Textbox(editor).edit().strip()
        finally:
            curses.curs_set(0)
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as error:
            show_error(screen, f"Invalid JSON: {error.msg}")
            continue
        if not isinstance(value, dict):
            show_error(screen, "Body JSON must be an object.")
            continue
        return value


def show_error(screen: curses.window, message: str) -> None:
    height, _width = screen.getmaxyx()
    put(screen, height - 2, 0, message, curses.A_BOLD)
    put(screen, height - 1, 0, "Press any key to continue", curses.A_DIM)
    screen.refresh()
    screen.getch()


def choose(screen: curses.window, title: str, labels: list[str]) -> int:
    if not labels:
        raise RuntimeError(f"No choices are available for {title}")
    selected = 0
    while True:
        screen.erase()
        height, _width = screen.getmaxyx()
        put(screen, 0, 0, "FieldLink radio console", curses.A_BOLD)
        put(screen, 2, 0, f"{title}  {selected + 1}/{len(labels)}", curses.A_BOLD)
        available = max(1, height - 6)
        start = min(
            max(0, selected - available + 1), max(0, len(labels) - available)
        )
        for row, index in enumerate(
            range(start, min(len(labels), start + available)), start=4
        ):
            label = labels[index]
            marker = "> " if index == selected else "  "
            style = curses.A_REVERSE if index == selected else curses.A_NORMAL
            put(screen, row, 0, marker + label, style)
        put(screen, height - 1, 0, "↑/↓ select   Enter confirm   q quit", curses.A_DIM)
        screen.refresh()
        key = screen.getch()
        if key in (ord("q"), 27):
            raise Cancelled()
        if key == curses.KEY_UP:
            selected = (selected - 1) % len(labels)
        elif key == curses.KEY_DOWN:
            selected = (selected + 1) % len(labels)
        elif key in (10, 13, curses.KEY_ENTER):
            return selected


def read_integer(
    screen: curses.window,
    prompt: str,
    default: int,
    maximum: int,
    minimum: int = 0,
) -> int:
    while True:
        screen.erase()
        put(screen, 0, 0, "FieldLink radio console", curses.A_BOLD)
        put(screen, 2, 0, prompt)
        put(screen, 4, 0, f"Value [{default}]: ")
        curses.echo()
        curses.curs_set(1)
        try:
            raw = screen.getstr(4, len(f"Value [{default}]: "), 16).decode().strip()
        finally:
            curses.noecho()
            curses.curs_set(0)
        if not raw:
            return default
        if raw.isdigit() and minimum <= int(raw) <= maximum:
            return int(raw)
        put(
            screen,
            6,
            0,
            f"Enter an integer from {minimum} through {maximum}.",
            curses.A_BOLD,
        )
        screen.getch()


def select_configuration(
    screen: curses.window,
    messages: list[MessageChoice],
    radios: list[RadioChoice],
    strategies: list[str],
    timeout_ms: int,
    output_root: Path,
) -> RunConfiguration:
    message = messages[choose(screen, "Message", [item.label for item in messages])]
    resource_request = (
        edit_resource_request(screen) if message.name == "resource" else None
    )
    radio_a = radios[
        choose(
            screen,
            "Source radio A. USB serial candidates, verified during preflight",
            [item.label for item in radios],
        )
    ]
    remaining = [item for item in radios if item.path != radio_a.path]
    radio_b = remaining[
        choose(
            screen,
            "Destination radio B. USB serial candidates, verified during preflight",
            [item.label for item in remaining],
        )
    ]
    if resource_request is None:
        payload_labels = [item.label for item in message.presets] + ["Custom size"]
        payload_index = choose(screen, "Payload", payload_labels)
        if payload_index == len(message.presets):
            payload_bytes = read_integer(
                screen,
                "Payload bytes. FieldLink will fragment it when needed.",
                message.default_payload_bytes,
                message.maximum_payload_bytes,
            )
        else:
            payload_bytes = message.presets[payload_index].payload_bytes
    else:
        payload_bytes = None
    retry_strategy = strategies[
        choose(screen, "Retry strategy", strategies)
    ]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root.resolve() / f"{stamp}-tui-{message.name}-{os.getpid()}"
    resource_request_path = (
        write_resource_request_file(resource_request)
        if resource_request is not None
        else None
    )
    return RunConfiguration(
        message=message,
        radio_a=radio_a,
        radio_b=radio_b,
        payload_bytes=payload_bytes,
        resource_request=resource_request,
        resource_request_path=resource_request_path,
        retry_strategy=retry_strategy,
        timeout_ms=timeout_ms,
        output=output,
    )


def write_resource_request_file(request: dict[str, Any]) -> Path:
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix="fieldlink-resource-",
        suffix=".json",
        delete=False,
    ) as handle:
        json.dump(request, handle, separators=(",", ":"), ensure_ascii=False)
        handle.write("\n")
        return Path(handle.name)


def pump(stream: TextIO, source: str, output: queue.Queue[tuple[str, str]]) -> None:
    for line in stream:
        output.put((source, line))


def read_new_events(path: Path, offset: int) -> tuple[int, list[dict[str, Any]]]:
    if not path.exists():
        return offset, []
    records = []
    with path.open("r", encoding="utf-8") as handle:
        handle.seek(offset)
        while True:
            line_offset = handle.tell()
            line = handle.readline()
            if not line:
                break
            if not line.endswith("\n"):
                return line_offset, records
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return handle.tell(), records


def load_summary(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if value.get("status") != "running" else None


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGINT)
        else:
            process.send_signal(signal.SIGINT)
    except ProcessLookupError:
        return


def kill_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
    except ProcessLookupError:
        return


def run_live(
    screen: curses.window,
    process: subprocess.Popen[str],
    config: RunConfiguration,
    transcript: RunTranscript | None = None,
) -> int:
    if process.stdout is None or process.stderr is None:
        raise RuntimeError("Could not capture FieldLink CLI output")
    output: queue.Queue[tuple[str, str]] = queue.Queue()
    threads = [
        threading.Thread(target=pump, args=(process.stdout, "cli", output), daemon=True),
        threading.Thread(target=pump, args=(process.stderr, "diag", output), daemon=True),
    ]
    for thread in threads:
        thread.start()
    view = RunView()
    event_offset = 0
    scroll_top = 0
    follow_tail = True
    cancelling = False
    stop_deadline: float | None = None
    screen.timeout(100)
    summary: dict[str, Any] | None = None
    drained = False
    try:
        while True:
            while True:
                try:
                    source, line = output.get_nowait()
                except queue.Empty:
                    break
                rendered = view.add_output(source, line)
                if transcript is not None:
                    transcript.append(rendered)
            event_offset, events = read_new_events(
                config.output / "events.jsonl", event_offset
            )
            for event in events:
                rendered = view.observe(event)
                if transcript is not None:
                    transcript.append(rendered)
            if process.poll() is not None and not drained:
                drained = True
                for thread in threads:
                    thread.join(timeout=0.2)
                while not output.empty():
                    source, line = output.get_nowait()
                    rendered = view.add_output(source, line)
                    if transcript is not None:
                        transcript.append(rendered)
                event_offset, events = read_new_events(
                    config.output / "events.jsonl", event_offset
                )
                for event in events:
                    rendered = view.observe(event)
                    if transcript is not None:
                        transcript.append(rendered)
                summary = load_summary(config.output / "summary.json")
            finished = process.poll() is not None
            scroll_top, maximum_scroll_top = draw_run(
                screen,
                config,
                view,
                summary,
                scroll_top,
                follow_tail,
                cancelling,
                finished,
            )
            key = screen.getch()
            if key == curses.KEY_UP:
                follow_tail = False
                scroll_top = max(0, scroll_top - 1)
            elif key == curses.KEY_DOWN:
                scroll_top = min(maximum_scroll_top, scroll_top + 1)
                follow_tail = scroll_top == maximum_scroll_top
            elif process.poll() is None and key in (ord("q"), 27):
                if not cancelling:
                    cancelling = True
                    stop_deadline = time.monotonic() + STOP_TIMEOUT_SECONDS
                    stop_process(process)
            elif process.poll() is not None and key != -1:
                return process.returncode or 0
            if (
                stop_deadline is not None
                and process.poll() is None
                and time.monotonic() >= stop_deadline
            ):
                kill_process(process)
                return process.wait()
            if process.poll() is not None and summary is None:
                summary = load_summary(config.output / "summary.json")
    finally:
        if transcript is not None:
            transcript.finish(
                received_message(view, summary, config),
                summary_lines(summary, view, config),
            )


def received_message(
    view: RunView,
    summary: dict[str, Any] | None,
    config: RunConfiguration,
) -> dict[str, Any] | None:
    correlated = selected_resource_response(view, summary, config)
    if correlated is not None or config.resource_request is not None:
        return correlated
    return view.received_message


def selected_resource_response(
    view: RunView,
    summary: dict[str, Any] | None,
    config: RunConfiguration,
) -> dict[str, Any] | None:
    if isinstance(summary, dict):
        candidate = summary.get("resourceResponse")
        if isinstance(candidate, dict):
            return candidate
    expected_request_id = (
        config.resource_request.get("request_id")
        if config.resource_request is not None
        else None
    )
    if view.resource_response is not None and (
        view.resource_response.get("request_id") == expected_request_id
    ):
        return view.resource_response
    return None


def draw_run(
    screen: curses.window,
    config: RunConfiguration,
    view: RunView,
    summary: dict[str, Any] | None,
    scroll_top: int,
    follow_tail: bool,
    cancelling: bool,
    finished: bool,
) -> tuple[int, int]:
    screen.erase()
    height, _width = screen.getmaxyx()
    state = "stopping" if cancelling and not finished else "finished" if finished else "running"
    document = run_document_lines(config, view, summary, state)
    content_height = max(0, height - 1)
    maximum_scroll_top = max(0, len(document) - content_height)
    if follow_tail:
        scroll_top = maximum_scroll_top
    else:
        scroll_top = min(scroll_top, maximum_scroll_top)
    for row, line in enumerate(
        document[scroll_top : scroll_top + content_height]
    ):
        style = (
            curses.A_BOLD
            if line.startswith("FieldLink ")
            or line in {"Live events", "Atlas response", "Run statistics"}
            else 0
        )
        put(screen, row, 0, line, style)
    footer = "↑/↓ scroll   any key exit" if finished else "↑/↓ scroll   q stop"
    put(screen, height - 1, 0, footer, curses.A_DIM)
    screen.refresh()
    return scroll_top, maximum_scroll_top


def run_document_lines(
    config: RunConfiguration,
    view: RunView,
    summary: dict[str, Any] | None,
    state: str,
) -> list[str]:
    if view.selected_channel is None:
        channel = "automatic channel selection"
    else:
        name = f" {view.selected_channel_name}" if view.selected_channel_name else ""
        channel = f"channel {view.selected_channel}{name}"
    if config.resource_request is None:
        input_description = f"Payload {config.payload_bytes:,} bytes"
    else:
        input_description = (
            f"Request {config.resource_request.get('operation', '?')} "
            f"{config.resource_request.get('resource_type', '?')}"
        )
    response = selected_resource_response(view, summary, config)
    response_lines = (
        []
        if response is None
        else [
            "",
            "Atlas response",
            *json.dumps(response, indent=2, ensure_ascii=False).splitlines(),
        ]
    )
    return [
        f"FieldLink {config.message.name}  {state}",
        f"A {config.radio_a.path}  →  {channel}  →  B {config.radio_b.path}",
        f"{input_description}   retry {config.retry_strategy}",
        "",
        "Live events",
        *view.logs,
        *response_lines,
        "",
        "Run statistics",
        *summary_lines(summary, view, config),
    ]


def put(screen: curses.window, row: int, column: int, text: str, style: int = 0) -> None:
    height, width = screen.getmaxyx()
    if row < 0 or row >= height or column >= width:
        return
    try:
        screen.addnstr(row, column, text, max(0, width - column - 1), style)
    except curses.error:
        pass


def tui(
    screen: curses.window,
    cli: FieldLinkCli,
    messages: list[MessageChoice],
    radios: list[RadioChoice],
    strategies: list[str],
    timeout_ms: int,
    output_root: Path,
) -> int:
    curses.curs_set(0)
    screen.keypad(True)
    config = select_configuration(
        screen, messages, radios, strategies, timeout_ms, output_root
    )
    process: subprocess.Popen[str] | None = None
    transcript: RunTranscript | None = None
    try:
        transcript = RunTranscript(RESULTS_TRANSCRIPT, config)
        process = cli.start_test(config)
        return run_live(screen, process, config, transcript)
    except Exception as error:
        if transcript is not None:
            transcript.append(f"[tui] {type(error).__name__}: {error}")
        raise
    finally:
        if process is not None and process.poll() is None:
            stop_process(process)
            try:
                process.wait(timeout=STOP_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                kill_process(process)
                process.wait()
        if transcript is not None:
            transcript.finish()
        if config.resource_request_path is not None:
            config.resource_request_path.unlink(missing_ok=True)


def parse_arguments(arguments: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--timeout-ms", type=timeout_milliseconds, default=30 * 60 * 1000
    )
    parser.add_argument("--output-root", type=Path, default=REPOSITORY / "results")
    return parser.parse_args(arguments)


def timeout_milliseconds(value: str) -> int:
    try:
        timeout = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if not 1 <= timeout <= MAX_TIMEOUT_MS:
        raise argparse.ArgumentTypeError(
            f"must be between 1 and {MAX_TIMEOUT_MS}"
        )
    return timeout


def main(arguments: list[str]) -> int:
    options = parse_arguments(arguments)
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        print("fieldlink_tui.py requires an interactive terminal", file=sys.stderr)
        return 2
    cli = FieldLinkCli()
    try:
        messages, strategies = cli.messages()
        radios = cli.radios()
        if not messages:
            raise RuntimeError("The FieldLink registry has no messages")
        if not strategies:
            raise RuntimeError("FieldLink has no retry strategies")
        if len(radios) < 2:
            raise RuntimeError("Connect two serial radios before starting the console")
        return curses.wrapper(
            tui,
            cli,
            messages,
            radios,
            strategies,
            options.timeout_ms,
            options.output_root,
        )
    except Cancelled:
        return 0
    except (OSError, RuntimeError) as error:
        print(f"fieldlink console: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

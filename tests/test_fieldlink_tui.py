import importlib.util
import io
import signal
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "tools" / "fieldlink_tui.py"
SPEC = importlib.util.spec_from_file_location("fieldlink_tui", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
TUI = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = TUI
SPEC.loader.exec_module(TUI)


class FieldLinkTuiTests(unittest.TestCase):
    def config(self):
        message = TUI.MessageChoice(1, "test", "normal", 64, 1_048_571, ())
        return TUI.RunConfiguration(
            message=message,
            radio_a=TUI.RadioChoice("/dev/cu.a", None, None),
            radio_b=TUI.RadioChoice("/dev/cu.b", None, None),
            payload_bytes=4096,
            resource_request=None,
            resource_request_path=None,
            retry_strategy="selective-window",
            timeout_ms=1000,
            output=Path("results/run"),
        )

    def test_builds_the_real_cli_command(self):
        command = TUI.build_test_command(self.config())
        self.assertEqual(command[:6], ["npm", "run", "--silent", "fieldlink", "--", "test"])
        self.assertIn("--allow-inbox-drain", command)
        self.assertEqual(command[command.index("--message") + 1], "test")
        self.assertEqual(command[command.index("--payload-size") + 1], "4096")
        self.assertNotIn("--channel", command)

    def test_builds_a_resource_request_command_without_a_payload_size(self):
        config = self.config()
        resource = TUI.RunConfiguration(
            message=TUI.MessageChoice(2, "resource", "normal", 32, 1_048_483, ()),
            radio_a=config.radio_a,
            radio_b=config.radio_b,
            payload_bytes=None,
            resource_request={
                "type": "resource",
                "kind": "request",
                "operation": "get",
                "request_id": "req-1",
                "resource_type": "task",
                "resource_id": "task-1",
            },
            resource_request_path=Path("/tmp/request.json"),
            retry_strategy=config.retry_strategy,
            timeout_ms=config.timeout_ms,
            output=config.output,
        )

        command = TUI.build_test_command(resource)

        self.assertEqual(
            command[command.index("--resource-request") + 1],
            "/tmp/request.json",
        )
        self.assertNotIn("--payload-size", command)

    def test_resource_draft_generates_operation_specific_json(self):
        draft = TUI.ResourceDraft(
            operation="list",
            resource_type="task",
            request_id="req-list",
            limit=25,
            cursor="next",
        )
        self.assertEqual(
            draft.message(),
            {
                "type": "resource",
                "kind": "request",
                "operation": "list",
                "request_id": "req-list",
                "resource_type": "task",
                "query": {"limit": 25, "cursor": "next"},
            },
        )

    def test_resource_editor_keeps_fields_inside_the_left_pane(self):
        draft = TUI.ResourceDraft(
            operation="create",
            request_id="request-" + "x" * 100,
            body={"entity_id": "entity-fieldlink-demo", "entity_type": "asset"},
        )
        fields = TUI.resource_fields(draft)
        screen = mock.Mock()
        screen.getmaxyx.return_value = (41, 103)

        with mock.patch.object(TUI, "put") as put:
            TUI.draw_resource_editor(screen, draft, fields, len(fields) - 1)

        left_fields = [
            call.args[3]
            for call in put.call_args_list
            if call.args[2] == 0 and 5 <= call.args[1] < 40
        ]
        self.assertGreaterEqual(len(left_fields), len(fields))
        self.assertTrue(all(line[48:51] == " | " for line in left_fields))
        body_row = left_fields[len(fields) - 1]
        self.assertIn("Body JSON    2 fields", body_row[:48])
        self.assertNotIn("entity-fieldlink-demo", body_row[:48])
        self.assertTrue(
            any("entity-fieldlink-demo" in line[51:] for line in left_fields)
        )

    def test_request_id_editor_enforces_the_utf8_byte_limit(self):
        screen = mock.Mock()
        screen.getstr.side_effect = [
            ("é" * 129).encode(),
            "request-valid".encode(),
        ]

        with (
            mock.patch.object(TUI.curses, "echo"),
            mock.patch.object(TUI.curses, "noecho"),
            mock.patch.object(TUI.curses, "curs_set"),
            mock.patch.object(TUI, "put") as put,
        ):
            result = TUI.read_text(
                screen,
                "Request ID",
                "request-default",
                allow_empty=False,
                maximum_utf8_bytes=TUI.MAX_RESOURCE_REQUEST_ID_BYTES,
            )

        self.assertEqual(result, "request-valid")
        self.assertTrue(
            any("at most 256 UTF-8 bytes" in call.args[3] for call in put.call_args_list)
        )

    def test_body_editor_draws_its_popup_before_accepting_input(self):
        screen = mock.Mock()
        screen.getmaxyx.return_value = (41, 103)
        popup = mock.Mock()
        editor = mock.Mock()
        popup.derwin.return_value = editor
        textbox = mock.Mock()
        textbox.edit.return_value = '{"alias":"updated"}'
        order = []
        popup.noutrefresh.side_effect = lambda: order.append("popup")
        editor.noutrefresh.side_effect = lambda: order.append("editor")
        textbox.edit.side_effect = lambda: (
            order.append("input") or '{"alias":"updated"}'
        )

        with (
            mock.patch.object(TUI.curses, "newwin", return_value=popup),
            mock.patch.object(TUI.curses, "doupdate", side_effect=lambda: order.append("screen")),
            mock.patch.object(TUI.curses, "curs_set"),
            mock.patch.object(TUI.curses.textpad, "Textbox", return_value=textbox),
            mock.patch.object(TUI, "put"),
        ):
            result = TUI.edit_json_object(screen, {"alias": "old"})

        self.assertEqual(result, {"alias": "updated"})
        self.assertEqual(order, ["popup", "editor", "screen", "input"])

    def test_body_editor_rejects_non_finite_json_numbers(self):
        screen = mock.Mock()
        screen.getmaxyx.return_value = (41, 103)
        popup = mock.Mock()
        popup.derwin.return_value = mock.Mock()
        textbox = mock.Mock()
        textbox.edit.side_effect = ['{"value":1e400}', '{"value":1}']

        with (
            mock.patch.object(TUI.curses, "newwin", return_value=popup),
            mock.patch.object(TUI.curses, "doupdate"),
            mock.patch.object(TUI.curses, "curs_set"),
            mock.patch.object(TUI.curses.textpad, "Textbox", return_value=textbox),
            mock.patch.object(TUI, "show_error") as show_error,
            mock.patch.object(TUI, "put"),
        ):
            result = TUI.edit_json_object(screen, {})

        self.assertEqual(result, {"value": 1})
        show_error.assert_called_once_with(
            screen, "Body JSON numbers must be finite."
        )

    def test_body_editor_preserves_json_that_does_not_fit(self):
        screen = mock.Mock()
        screen.getmaxyx.return_value = (8, 40)
        popup = mock.Mock()
        popup.derwin.return_value = mock.Mock()
        current = {"notes": "x" * 500}

        with (
            mock.patch.object(TUI.curses, "newwin", return_value=popup),
            mock.patch.object(TUI, "show_error") as show_error,
            mock.patch.object(TUI.curses.textpad, "Textbox") as textbox,
            mock.patch.object(TUI, "put"),
        ):
            result = TUI.edit_json_object(screen, current)

        self.assertIs(result, current)
        show_error.assert_called_once()
        textbox.assert_not_called()

    def test_body_editor_preserves_json_when_curses_rejects_it(self):
        screen = mock.Mock()
        screen.getmaxyx.return_value = (41, 103)
        popup = mock.Mock()
        editor = mock.Mock()
        editor.addstr.side_effect = TUI.curses.error
        popup.derwin.return_value = editor
        current = {"notes": "wide characters: 漢字"}

        with (
            mock.patch.object(TUI.curses, "newwin", return_value=popup),
            mock.patch.object(TUI, "show_error") as show_error,
            mock.patch.object(TUI.curses.textpad, "Textbox") as textbox,
            mock.patch.object(TUI, "put"),
        ):
            result = TUI.edit_json_object(screen, current)

        self.assertIs(result, current)
        show_error.assert_called_once()
        textbox.assert_not_called()

    def test_marks_radio_candidates_unverified(self):
        choice = TUI.RadioChoice("/dev/cu.usbserial-4", "Silicon Labs", "0001")
        self.assertEqual(
            choice.label,
            "/dev/cu.usbserial-4  Silicon Labs | 0001  unverified",
        )

    def test_reports_cli_discovery_timeout(self):
        with mock.patch.object(
            TUI.subprocess,
            "run",
            side_effect=TUI.subprocess.TimeoutExpired(["fieldlink"], 30),
        ):
            with self.assertRaisesRegex(RuntimeError, "within 30 seconds"):
                TUI.FieldLinkCli().run_json("radios", "list", "--json")

    def test_rejects_timeout_above_cli_maximum(self):
        with self.assertRaises(TUI.argparse.ArgumentTypeError):
            TUI.timeout_milliseconds(str(TUI.MAX_TIMEOUT_MS + 1))

    def test_stop_process_tolerates_an_exit_race(self):
        process = mock.Mock(pid=123)
        process.poll.return_value = None
        with mock.patch.object(
            TUI.os, "killpg", side_effect=ProcessLookupError
        ):
            TUI.stop_process(process)

    def test_tui_stops_and_reaps_the_test_after_an_exception(self):
        screen = mock.Mock()
        cli = mock.Mock()
        process = mock.Mock()
        process.poll.return_value = None
        cli.start_test.return_value = process
        with (
            mock.patch.object(TUI.curses, "curs_set"),
            mock.patch.object(TUI, "select_configuration", return_value=self.config()),
            mock.patch.object(TUI, "run_live", side_effect=KeyboardInterrupt),
            mock.patch.object(TUI, "stop_process") as stop_process,
            mock.patch.object(TUI, "RunTranscript"),
        ):
            with self.assertRaises(KeyboardInterrupt):
                TUI.tui(screen, cli, [], [], [], 1000, Path("results"))

        stop_process.assert_called_once_with(process)
        process.wait.assert_called_once_with(timeout=TUI.STOP_TIMEOUT_SECONDS)

    def test_tui_removes_the_request_file_when_transcript_setup_fails(self):
        config = self.config()
        with tempfile.TemporaryDirectory() as directory:
            request_path = Path(directory) / "request.json"
            request_path.write_text("{}", encoding="utf-8")
            resource = TUI.RunConfiguration(
                message=TUI.MessageChoice(2, "resource", "normal", 32, 1_048_483, ()),
                radio_a=config.radio_a,
                radio_b=config.radio_b,
                payload_bytes=None,
                resource_request={"request_id": "req-1"},
                resource_request_path=request_path,
                retry_strategy=config.retry_strategy,
                timeout_ms=config.timeout_ms,
                output=config.output,
            )
            with (
                mock.patch.object(TUI.curses, "curs_set"),
                mock.patch.object(TUI, "select_configuration", return_value=resource),
                mock.patch.object(
                    TUI, "RunTranscript", side_effect=OSError("transcript failed")
                ),
            ):
                with self.assertRaisesRegex(OSError, "transcript failed"):
                    TUI.tui(mock.Mock(), mock.Mock(), [], [], [], 1000, Path("results"))

            self.assertFalse(request_path.exists())

    def test_prefers_the_summary_resource_response(self):
        view = TUI.RunView(
            received_message={
                "type": "resource",
                "kind": "response",
                "request_id": "stale",
                "status": 200,
            }
        )
        correlated = {
            "type": "resource",
            "kind": "response",
            "request_id": "expected",
            "status": 200,
        }

        self.assertEqual(
            TUI.received_message(
                view, {"resourceResponse": correlated}, self.config()
            ),
            correlated,
        )

    def test_results_transcript_overwrites_and_orders_the_run(self):
        config = self.config()
        request = {
            "type": "resource",
            "kind": "request",
            "operation": "list",
            "request_id": "req-list",
            "resource_type": "task",
            "query": {"limit": 25},
        }
        resource = TUI.RunConfiguration(
            message=TUI.MessageChoice(2, "resource", "normal", 32, 1_048_483, ()),
            radio_a=config.radio_a,
            radio_b=config.radio_b,
            payload_bytes=None,
            resource_request=request,
            resource_request_path=Path("/tmp/request.json"),
            retry_strategy=config.retry_strategy,
            timeout_ms=config.timeout_ms,
            output=config.output,
        )
        response = {
            "type": "resource",
            "kind": "response",
            "request_id": "req-list",
            "status": 200,
            "body": {"items": [{"task_id": "task-1"}]},
        }

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "results.txt"
            path.write_text("old run", encoding="utf-8")
            transcript = TUI.RunTranscript(path, resource)
            transcript.append("12:00:00 [A] sent request")
            transcript.append("12:00:01 [A] received response")
            transcript.finish(response, ["Status passed"])
            transcript.finish()
            rendered = path.read_text(encoding="utf-8")

        self.assertNotIn("old run", rendered)
        self.assertLess(
            rendered.index('\"operation\": \"list\"'), rendered.index("RUN LOG")
        )
        self.assertLess(rendered.index("RUN LOG"), rendered.index("sent request"))
        self.assertLess(rendered.index("sent request"), rendered.index("RUN RESULT"))
        self.assertLess(
            rendered.index("RUN RESULT"), rendered.index("RECEIVED MESSAGE")
        )
        self.assertTrue(rendered.rstrip().endswith("}"))
        self.assertIn('\"task_id\": \"task-1\"', rendered)

    def test_live_stop_force_kills_after_the_deadline(self):
        screen = mock.Mock()
        screen.getch.return_value = ord("q")
        process = mock.Mock(
            stdout=io.StringIO(), stderr=io.StringIO(), returncode=None
        )
        process.poll.return_value = None
        process.wait.return_value = -signal.SIGKILL
        with (
            mock.patch.object(TUI, "draw_run", return_value=(0, 0)),
            mock.patch.object(TUI, "read_new_events", return_value=(0, [])),
            mock.patch.object(TUI, "stop_process") as stop_process,
            mock.patch.object(TUI, "kill_process") as kill_process,
            mock.patch.object(
                TUI.time,
                "monotonic",
                side_effect=[100.0, 100.0 + TUI.STOP_TIMEOUT_SECONDS],
            ),
        ):
            result = TUI.run_live(screen, process, self.config())

        self.assertEqual(result, -signal.SIGKILL)
        stop_process.assert_called_once_with(process)
        kill_process.assert_called_once_with(process)
        process.wait.assert_called_once_with()

    def test_renders_chunk_progress_and_statistics(self):
        view = TUI.RunView()
        view.observe(
            {
                "type": "channel-scan-started",
                "data": {"scope": "all-available"},
            }
        )
        view.observe(
            {
                "type": "channel-selected",
                "data": {
                    "mode": "automatic",
                    "channel": {"index": 2, "name": "fieldlink"},
                },
            }
        )
        view.observe(
            {
                "at": "2026-08-24T12:00:00.000Z",
                "type": "node-event",
                "data": {
                    "radio": "A",
                    "event": {
                        "type": "transfer-started",
                        "logicalId": "1",
                        "encodedBytes": 4101,
                        "fragmentCount": 32,
                    },
                },
            }
        )
        view.observe(
            {
                "at": "2026-08-24T12:00:01.000Z",
                "type": "node-event",
                "data": {
                    "radio": "A",
                    "event": {
                        "type": "fragment-sent",
                        "logicalId": "1",
                        "fragmentIndex": 0,
                    },
                },
            }
        )
        self.assertEqual(view.selected_channel, 2)
        self.assertIn("every available", view.logs[0])
        self.assertIn("channel 2 fieldlink", view.logs[1])
        self.assertIn("32 fragments", view.logs[2])
        self.assertIn("chunk 1/32", view.logs[3])

        summary = {
            "status": "passed",
            "condition": "recovered",
            "verification": {
                "correlation": "matched",
                "responseDigest": "verified",
            },
            "elapsedMs": 2000,
            "selectedChannel": {"index": 2, "name": "fieldlink"},
            "request": {
                "delivery": "transfer",
                "encodedBytes": 4101,
                "fragments": 32,
                "transferOpenRetries": 0,
                "completionRetries": 0,
                "retransmissions": 0,
                "receiptRequests": 4,
                "receiptRequestRetries": 0,
                "receipts": 4,
                "durationMs": 1500,
            },
            "response": {
                "delivery": "transfer",
                "encodedBytes": 4101,
                "fragments": 32,
                "transferOpenRetries": 0,
                "completionRetries": 1,
                "retransmissions": 1,
                "receiptRequests": 5,
                "receiptRequestRetries": 1,
                "receipts": 4,
                "durationMs": 1750,
            },
        }
        rendered = "\n".join(TUI.summary_lines(summary, view, self.config()))
        self.assertIn("passed", rendered)
        self.assertIn("MeshCore channel 2 fieldlink", rendered)
        self.assertIn("Response transfer", rendered)
        self.assertIn("retransmissions 1", rendered)
        self.assertIn("completion retries 1", rendered)
        self.assertIn("condition recovered", rendered)
        self.assertIn("receipt requests 4", rendered)
        self.assertIn("receipt requests 5", rendered)
        self.assertIn("request retries 1", rendered)
        self.assertIn("digest verified", rendered)
        self.assertIn("Request payload per echo round trip 2.00 KiB/s", rendered)

        document = TUI.run_document_lines(
            self.config(), view, summary, "finished"
        )
        self.assertLess(document.index("Live events"), document.index(view.logs[0]))
        self.assertLess(document.index(view.logs[-1]), document.index("Run statistics"))
        self.assertLess(
            document.index("Run statistics"),
            next(index for index, line in enumerate(document) if line.startswith("Status ")),
        )

    def test_renders_the_resource_response_json(self):
        config = self.config()
        resource = TUI.RunConfiguration(
            message=TUI.MessageChoice(2, "resource", "normal", 32, 1_048_483, ()),
            radio_a=config.radio_a,
            radio_b=config.radio_b,
            payload_bytes=None,
            resource_request={
                "type": "resource",
                "kind": "request",
                "operation": "get",
                "request_id": "req-1",
                "resource_type": "task",
                "resource_id": "task-1",
            },
            resource_request_path=Path("/tmp/request.json"),
            retry_strategy=config.retry_strategy,
            timeout_ms=config.timeout_ms,
            output=config.output,
        )
        view = TUI.RunView()
        view.observe(
            {
                "type": "message",
                "data": {
                    "radio": "A",
                    "message": {
                        "message": {
                            "type": "resource",
                            "kind": "response",
                            "request_id": "req-1",
                            "status": 200,
                            "body": {"task_id": "task-1"},
                        }
                    },
                },
            }
        )

        rendered = "\n".join(
            TUI.run_document_lines(resource, view, None, "running")
        )
        self.assertIn("Request get task", rendered)
        self.assertIn("Atlas response", rendered)
        self.assertIn('"task_id": "task-1"', rendered)

    def test_reads_only_complete_jsonl_records(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            complete = '{"type":"test-passed","data":{}}\n'
            path.write_text(complete + '{"type":"partial"', encoding="utf-8")
            offset, records = TUI.read_new_events(path, 0)
            self.assertEqual(offset, len(complete))
            self.assertEqual([record["type"] for record in records], ["test-passed"])


if __name__ == "__main__":
    unittest.main()

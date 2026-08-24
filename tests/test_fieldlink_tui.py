import importlib.util
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
        ):
            with self.assertRaises(KeyboardInterrupt):
                TUI.tui(screen, cli, [], [], [], 1000, Path("results"))

        stop_process.assert_called_once_with(process)
        process.wait.assert_called_once_with(timeout=TUI.STOP_TIMEOUT_SECONDS)

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
            "integrity": "matched",
            "elapsedMs": 2000,
            "selectedChannel": {"index": 2, "name": "fieldlink"},
            "request": {
                "delivery": "transfer",
                "encodedBytes": 4101,
                "fragments": 32,
                "retransmissions": 0,
                "receipts": 4,
                "durationMs": 1500,
            },
        }
        rendered = "\n".join(TUI.summary_lines(summary, view, self.config()))
        self.assertIn("passed", rendered)
        self.assertIn("MeshCore channel 2 fieldlink", rendered)
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

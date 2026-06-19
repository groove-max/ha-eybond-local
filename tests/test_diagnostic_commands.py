from __future__ import annotations

from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from custom_components.eybond_local.support.diagnostic_commands import (
    AsciiCommand,
    Directives,
    ReadCommand,
    ScenarioError,
    SleepCommand,
    WriteBitCommand,
    WriteCommand,
    parse_scenario,
)


class ScenarioParseTests(unittest.TestCase):
    def test_blank_lines_and_comments_are_ignored(self) -> None:
        scenario = parse_scenario(
            "\n"
            "   \n"
            "# a comment\n"
            "   # indented comment\n"
            "read 10\n"
        )
        self.assertEqual(len(scenario.commands), 1)
        self.assertIsInstance(scenario.commands[0], ReadCommand)
        self.assertEqual(scenario.commands[0].line, 5)

    def test_hash_inside_ascii_is_not_a_comment(self) -> None:
        scenario = parse_scenario("driver pi30\nascii Q#FOO\n")
        command = scenario.commands[0]
        assert isinstance(command, AsciiCommand)
        self.assertEqual(command.command, "Q#FOO")

    def test_decimal_and_hex_numbers(self) -> None:
        scenario = parse_scenario(
            "devcode 1\n"
            "collector_addr 0xFF\n"
            "device_addr 4\n"
            "read 0x00AB 2\n"
        )
        self.assertEqual(scenario.directives.collector_addr, 255)
        command = scenario.commands[0]
        assert isinstance(command, ReadCommand)
        self.assertEqual(command.register, 0xAB)
        self.assertEqual(command.count, 2)

    def test_all_directives(self) -> None:
        scenario = parse_scenario(
            "driver modbus_smg\n"
            "devcode 1\n"
            "collector_addr 0xFF\n"
            "device_addr 4\n"
            "stop_on_error false\n"
            "operation_timeout 2.5\n"
            "read 10\n"
        )
        self.assertEqual(
            scenario.directives,
            Directives(
                driver="modbus_smg",
                devcode=1,
                collector_addr=255,
                device_addr=4,
                stop_on_error=False,
                operation_timeout=2.5,
            ),
        )

    def test_directive_after_first_command_is_rejected(self) -> None:
        with self.assertRaises(ScenarioError) as ctx:
            parse_scenario("read 10\ndevcode 1\n")
        self.assertEqual(ctx.exception.line, 2)
        self.assertIn("must appear before the first command", str(ctx.exception))

    def test_parse_read_default_count(self) -> None:
        command = parse_scenario("read 171\n").commands[0]
        assert isinstance(command, ReadCommand)
        self.assertEqual((command.register, command.count), (171, 1))

    def test_parse_write_single_and_multiple(self) -> None:
        single = parse_scenario("write 354 1\n").commands[0]
        assert isinstance(single, WriteCommand)
        self.assertEqual(single.values, (1,))
        multi = parse_scenario("write 100 1 2 0xFFFF\n").commands[0]
        assert isinstance(multi, WriteCommand)
        self.assertEqual(multi.values, (1, 2, 0xFFFF))

    def test_parse_write_bit(self) -> None:
        command = parse_scenario("write_bit 354 0 1\n").commands[0]
        assert isinstance(command, WriteBitCommand)
        self.assertEqual((command.register, command.bit_index, command.bit_value), (354, 0, 1))

    def test_parse_ascii_preserves_arguments(self) -> None:
        command = parse_scenario("driver pi30\nascii POP 02\n").commands[0]
        assert isinstance(command, AsciiCommand)
        self.assertEqual(command.command, "POP 02")

    def test_parse_sleep(self) -> None:
        command = parse_scenario("sleep 2000\n").commands[0]
        assert isinstance(command, SleepCommand)
        self.assertEqual(command.milliseconds, 2000)

    def test_command_kinds(self) -> None:
        scenario = parse_scenario(
            "read 1\nwrite 1 1\nwrite_bit 1 0 1\nsleep 1\n"
        )
        self.assertEqual(
            [c.kind for c in scenario.commands],
            ["modbus_read", "modbus_write", "modbus_write_bit", "sleep"],
        )

    def test_unknown_command_is_error_with_line(self) -> None:
        with self.assertRaises(ScenarioError) as ctx:
            parse_scenario("read 10\nreads 5\n")
        self.assertEqual(ctx.exception.line, 2)
        self.assertIn("unknown command 'reads'", str(ctx.exception))

    def test_unknown_token_is_not_silently_ignored(self) -> None:
        with self.assertRaises(ScenarioError):
            parse_scenario("frobnicate 1 2 3\n")

    def test_wrong_argument_count(self) -> None:
        with self.assertRaises(ScenarioError) as ctx:
            parse_scenario("read\n")
        self.assertEqual(ctx.exception.line, 1)
        with self.assertRaises(ScenarioError):
            parse_scenario("write 354\n")  # needs at least one value
        with self.assertRaises(ScenarioError):
            parse_scenario("write_bit 354 0\n")  # needs three args

    def test_register_range(self) -> None:
        with self.assertRaises(ScenarioError) as ctx:
            parse_scenario("read 70000\n")
        self.assertIn("register must be between 0 and 65535", str(ctx.exception))

    def test_value_range(self) -> None:
        with self.assertRaises(ScenarioError):
            parse_scenario("write 10 70000\n")

    def test_bit_index_range(self) -> None:
        with self.assertRaises(ScenarioError) as ctx:
            parse_scenario("write_bit 10 16 1\n")
        self.assertIn("bit index must be between 0 and 15", str(ctx.exception))

    def test_bit_value_must_be_zero_or_one(self) -> None:
        with self.assertRaises(ScenarioError):
            parse_scenario("write_bit 10 0 2\n")

    def test_read_count_must_be_positive(self) -> None:
        with self.assertRaises(ScenarioError):
            parse_scenario("read 10 0\n")

    def test_sleep_must_not_be_negative(self) -> None:
        with self.assertRaises(ScenarioError):
            parse_scenario("sleep -1\n")

    def test_operation_timeout_must_be_positive(self) -> None:
        with self.assertRaises(ScenarioError):
            parse_scenario("operation_timeout 0\n")

    def test_stop_on_error_must_be_boolean(self) -> None:
        with self.assertRaises(ScenarioError):
            parse_scenario("stop_on_error maybe\n")

    def test_non_integer_register_reports_line(self) -> None:
        with self.assertRaises(ScenarioError) as ctx:
            parse_scenario("read abc\n")
        self.assertEqual(ctx.exception.line, 1)

    def test_example_scenario_from_spec(self) -> None:
        scenario = parse_scenario(
            "# Temporarily probe SMG at a non-standard address\n"
            "driver modbus_smg\n"
            "devcode 1\n"
            "collector_addr 0xFF\n"
            "device_addr 4\n"
            "stop_on_error true\n"
            "\n"
            "read 171 14\n"
            "read 643 2\n"
            "write_bit 354 0 1\n"
            "sleep 2000\n"
            "read 354\n"
        )
        self.assertEqual(scenario.directives.driver, "modbus_smg")
        self.assertEqual(scenario.directives.stop_on_error, True)
        self.assertEqual(len(scenario.commands), 5)
        self.assertEqual(scenario.commands[0].line, 8)


if __name__ == "__main__":
    unittest.main()

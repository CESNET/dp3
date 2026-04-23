import argparse
import os
import unittest
from unittest.mock import patch

from argcomplete.finders import CompletionFinder

from dp3.bin.sh import init_parser, render_completion_shellcode
from dp3.bin.shcmd.common import complete_entity_type_names


class TestShCompletion(unittest.TestCase):
    def _create_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog="dpsh")
        init_parser(parser)
        return parser

    def _finder_with_completions(self, comp_words, prefix):
        finder = CompletionFinder(self._create_parser(), always_complete_options=False)
        values = finder._get_completions(comp_words, prefix, "", None)
        return finder, values

    def _parse_args(self, argv):
        return self._create_parser().parse_args(argv)

    def _get_completions(self, comp_words, prefix):
        return self._finder_with_completions(comp_words, prefix)[1]

    def test_top_level_completion_includes_entity_commands(self):
        values = self._get_completions(["dpsh"], "e")
        self.assertIn("entities", values)
        self.assertIn("entity", values)

    def test_entity_type_completion_uses_config(self):
        values = self._get_completions(["dpsh", "--config", "tests/test_config", "entity"], "A")
        self.assertEqual(["A "], values)

    def test_entity_type_completion_uses_entity_catalog_fallback(self):
        args = argparse.Namespace(
            config=os.path.join("tests", "test_common"), url=None, timeout=5.0
        )
        with (
            patch("dp3.bin.shcmd.common.load_completion_model_spec", return_value=None),
            patch(
                "dp3.bin.shcmd.common.load_completion_entity_catalog",
                return_value={"A": {"attribs": ["data1"]}},
            ),
        ):
            values = complete_entity_type_names("A", args)
        self.assertEqual({"A": "Configured entity type."}, values)

    def test_entity_scope_completion_includes_id_subcommand(self):
        values = self._get_completions(["dpsh", "--config", "tests/test_config", "entity", "A"], "")
        self.assertIn("id", values)
        self.assertIn("list", values)

    def test_entity_id_completion_includes_eid_placeholder(self):
        values = self._get_completions(
            ["dpsh", "--config", "tests/test_config", "entity", "A", "id"], ""
        )
        self.assertTrue(any("EID" in value for value in values))

    def test_entity_list_option_completion(self):
        values = self._get_completions(
            ["dpsh", "--config", "tests/test_config", "entity", "A", "list"],
            "--",
        )
        self.assertIn("--format", values)
        self.assertIn("--limit", values)

    def test_global_short_options_parse(self):
        args = self._parse_args(
            ["-c", "tests/test_config", "-u", "http://localhost:5000", "-t", "1.5", "health"]
        )
        self.assertEqual("tests/test_config", args.config)
        self.assertEqual("http://localhost:5000", args.url)
        self.assertEqual(1.5, args.timeout)

    def test_entity_short_options_parse(self):
        args = self._parse_args(
            [
                "entity",
                "A",
                "list",
                "-q",
                '{"k":"v"}',
                "-j",
                '{"x":1}',
                "-a",
                "data1",
                "-s",
                "1",
                "-l",
                "2",
                "-F",
                "ndjson",
            ]
        )
        parsed_args, exit_code = args.prepare_args(args)
        self.assertIsNone(exit_code)
        self.assertEqual('{"k":"v"}', parsed_args.fulltext_json)
        self.assertEqual('{"x":1}', parsed_args.filter_json)
        self.assertEqual("data1", parsed_args.has_attr)
        self.assertEqual(1, parsed_args.skip)
        self.assertEqual(2, parsed_args.limit)
        self.assertEqual("ndjson", parsed_args.format)

    def test_telemetry_short_options_parse(self):
        args = self._parse_args(
            [
                "telemetry",
                "metadata",
                "-m",
                "SnapShooter",
                "-f",
                "2024-01-01",
                "-t",
                "2024-01-02",
                "-s",
                "1",
                "-l",
                "2",
                "-S",
                "oldest",
                "-F",
                "ndjson",
            ]
        )
        self.assertEqual("SnapShooter", args.module)
        self.assertEqual("2024-01-01", args.date_from)
        self.assertEqual("2024-01-02", args.date_to)
        self.assertEqual(1, args.skip)
        self.assertEqual(2, args.limit)
        self.assertEqual("oldest", args.sort)
        self.assertEqual("ndjson", args.format)

    def test_snapshot_option_completion(self):
        values = self._get_completions(
            ["dpsh", "--config", "tests/test_config", "entity", "A", "id", "10", "snapshots"],
            "--",
        )
        self.assertIn("--from", values)
        self.assertIn("--to", values)
        self.assertIn("--limit", values)

    def test_entity_list_option_completion_includes_descriptions(self):
        finder, values = self._finder_with_completions(
            ["dpsh", "--config", "tests/test_config", "entity", "A", "list"],
            "--",
        )
        self.assertIn("--format", values)
        self.assertEqual(
            "Choose JSON or NDJSON output.", finder._display_completions.get("--format")
        )

    def test_snapshot_option_completion_includes_descriptions(self):
        finder, values = self._finder_with_completions(
            ["dpsh", "--config", "tests/test_config", "entity", "A", "id", "10", "snapshots"],
            "--",
        )
        self.assertIn("--from", values)
        self.assertEqual(
            "Lower bound of the snapshot time range.",
            finder._display_completions.get("--from"),
        )

    def test_entity_type_attr_values_completion_uses_config(self):
        values = self._get_completions(
            ["dpsh", "--config", "tests/test_config", "entity", "A", "attr-values"], "d"
        )
        self.assertIn("data1", values)
        self.assertIn("data2", values)
        self.assertIn("data3", values)

    def test_entity_type_completion_includes_descriptions(self):
        finder, values = self._finder_with_completions(
            ["dpsh", "--config", "tests/test_config", "entity"], "A"
        )
        self.assertIn("A ", values)
        self.assertTrue(finder._display_completions.get("A", "").startswith("test entity A"))

    def test_entity_instance_attr_action_completion(self):
        values = self._get_completions(
            [
                "dpsh",
                "--config",
                "tests/test_config",
                "entity",
                "A",
                "id",
                "entity-1",
                "attr",
                "data1",
            ],
            "",
        )
        self.assertIn("get", values)
        self.assertIn("set", values)

    def test_type_query_flag_value_completion_uses_attrs(self):
        values = self._get_completions(
            ["dpsh", "--config", "tests/test_config", "entity", "A", "list", "--has-attr"],
            "d",
        )
        self.assertIn("data1", values)
        self.assertIn("data2", values)
        self.assertIn("data3", values)

    def test_bash_completion_script_registers_commands(self):
        script = render_completion_shellcode("bash", ["dp3", "appsh"])
        self.assertIn("complete -o nospace", script)
        self.assertIn("dp3", script)
        self.assertIn("appsh", script)

    def test_zsh_completion_script_registers_commands(self):
        script = render_completion_shellcode("zsh", ["dp3", "appsh"])
        self.assertIn("compdef _python_argcomplete dp3 appsh", script)

    def test_fish_completion_script_registers_commands(self):
        script = render_completion_shellcode("fish", ["dp3", "appsh"])
        self.assertIn("complete --command dp3", script)
        self.assertIn("complete --command appsh", script)

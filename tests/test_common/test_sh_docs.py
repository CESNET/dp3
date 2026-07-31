import argparse
import unittest

from dp3.bin.sh import build_parser
from dp3.bin.shcmd.entity.etype import build_parser as build_entity_type_parser
from macros import (
    _format_parser_help,
    _render_argparse_tree,
    _subcommand_parser_items,
    dp3_sh_help,
)


class TestShDocs(unittest.TestCase):
    def test_standalone_parser_uses_dp3_sh_program_name(self):
        parser = build_parser()

        self.assertEqual("dp3 sh", parser.prog)
        self.assertEqual("health", parser.parse_args(["health"]).sh_command)

    def test_generated_help_uses_argparse_output_without_root_heading(self):
        rendered = dp3_sh_help()

        self.assertIn("usage: dp3 sh", rendered)
        self.assertNotIn("## `dp3 sh`", rendered)
        self.assertIn("## `telemetry`", rendered)
        self.assertIn("### `metadata`", rendered)
        self.assertNotIn("### `dp3 sh telemetry`", rendered)

    def test_generated_help_places_rich_example_before_help_block(self):
        rendered = dp3_sh_help()

        self.assertIn(
            "### `metadata`\n\nBrowse diagnostic records produced by internal periodic processes. "
            "Time bounds are ISO 8601 timestamps.\n\n```shell\n"
            "dp3 sh telemetry metadata --module SnapShooter "
            "--from 2024-01-01T00:00:00Z --sort oldest --limit 100 --format ndjson\n"
            "```\n\n```text\n",
            rendered,
        )

    def test_every_command_has_sentence_description(self):
        def assert_descriptions(parser):
            for _name, child in _subcommand_parser_items(parser):
                summary = (child.description or "").partition("\n\n")[0]
                self.assertTrue(summary.endswith("."), child.prog)
                assert_descriptions(child)

        assert_descriptions(build_parser())
        assert_descriptions(build_entity_type_parser("<ETYPE>"))

    def test_examples_are_limited_to_commands_where_they_explain_formats_or_options(self):
        rendered = dp3_sh_help()

        self.assertNotIn("dp3 sh health\n", rendered)
        self.assertNotIn("dp3 sh entity <ETYPE> list --limit", rendered)
        self.assertIn(
            "dp3 sh entity <ETYPE> id EID snapshots --from 2024-01-01T00:00:00Z",
            rendered,
        )
        self.assertIn(
            "dp3 sh telemetry event-counts --group te --interval 5m --both",
            rendered,
        )

    def test_formatted_help_omits_help_option_and_empty_options_section(self):
        parser = argparse.ArgumentParser(prog="example")

        rendered = _format_parser_help(parser)

        self.assertIn("usage: example [-h]", rendered)
        self.assertNotIn("-h, --help", rendered)
        self.assertNotIn("options:", rendered)

    def test_generated_help_flattens_dynamic_entity_selectors(self):
        rendered = dp3_sh_help()

        self.assertNotIn("### `<ETYPE>`", rendered)
        self.assertNotIn("### `id`", rendered)
        self.assertIn("### `list`", rendered)
        self.assertIn("### `id snapshots`", rendered)
        self.assertIn("### `id attr`", rendered)
        self.assertIn("#### `get`", rendered)
        self.assertNotIn("#####", rendered)
        self.assertIn("usage: dp3 sh entity <ETYPE> id EID snapshots", rendered)
        self.assertIn("--body-json BODY_JSON", rendered)

    def test_renderer_deduplicates_subcommand_aliases(self):
        parser = argparse.ArgumentParser(prog="example")
        commands = parser.add_subparsers()
        commands.add_parser("status", aliases=["s"])

        rendered = _render_argparse_tree(parser)

        self.assertEqual(1, rendered.count("### `status`"))
        self.assertNotIn("### `s`", rendered)


if __name__ == "__main__":
    unittest.main()

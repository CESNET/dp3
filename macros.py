import argparse
from urllib.parse import quote_plus

from dp3.bin.sh import build_parser as build_sh_parser
from dp3.bin.shcmd.entity.etype import build_parser as build_entity_type_parser

CESNET_APP_REPOS = (
    "CESNET/Amfora",
    "CESNET/ADiCT",
    "CESNET/NERD2",
)

GITHUB_CODE_SEARCH_URL = "https://github.com/search?q={query}&type=code"


def _repo_query(repos: tuple[str, ...]) -> str:
    return " OR ".join(f"repo:{repo}" for repo in repos)


def _github_code_search_link(label: str, query: str) -> str:
    encoded_query = quote_plus(query)
    return f"[{label}]({GITHUB_CODE_SEARCH_URL.format(query=encoded_query)})"


def _subcommand_parser_items(
    parser: argparse.ArgumentParser,
) -> list[tuple[str, argparse.ArgumentParser]]:
    """Return unique subcommand names and parsers in argparse display order."""
    subcommands = []
    seen = set()
    for action in parser._actions:
        if not isinstance(action, argparse._SubParsersAction):
            continue
        for name, subparser in action.choices.items():
            parser_id = id(subparser)
            if parser_id not in seen:
                subcommands.append((name, subparser))
                seen.add(parser_id)
    return subcommands


def _subcommand_parsers(
    parser: argparse.ArgumentParser,
) -> list[argparse.ArgumentParser]:
    """Return unique subcommand parsers in their argparse display order."""
    return [subparser for _, subparser in _subcommand_parser_items(parser)]


def _format_parser_help(parser: argparse.ArgumentParser) -> str:
    """Format argparse help without descriptions or the help-option entry."""
    formatter = parser._get_formatter()
    formatter.add_usage(parser.usage, parser._actions, parser._mutually_exclusive_groups)
    for action_group in parser._action_groups:
        actions = [action for action in action_group._group_actions if action.dest != "help"]
        if not actions:
            continue
        formatter.start_section(action_group.title)
        formatter.add_text(action_group.description)
        formatter.add_arguments(actions)
        formatter.end_section()
    return formatter.format_help().rstrip()


def _render_argparse_tree(
    parser: argparse.ArgumentParser,
    heading_level: int = 2,
    extra_children: dict[str, list[argparse.ArgumentParser]] | None = None,
    heading: str | None = None,
    flattened_parsers: dict[str, str] | None = None,
) -> str:
    """Render a parser and its subcommands as Markdown help sections."""
    extra_children = extra_children or {}
    flattened_parsers = flattened_parsers or {}
    children = _subcommand_parser_items(parser)
    children.extend(
        (
            child.prog.removeprefix(f"{parser.prog} "),
            child,
        )
        for child in extra_children.get(parser.prog, [])
    )

    if parser.prog in flattened_parsers:
        heading_prefix = flattened_parsers[parser.prog]
        return "\n\n".join(
            _render_argparse_tree(
                child,
                heading_level,
                extra_children,
                heading=" ".join(filter(None, (heading_prefix, child_name))),
                flattened_parsers=flattened_parsers,
            )
            for child_name, child in children
        )

    markdown_heading_level = min(heading_level, 6)
    sections = [f"{'#' * markdown_heading_level} `{heading or parser.prog}`", ""]
    if parser.description:
        sections.extend([parser.description, ""])
    sections.extend(["```text", _format_parser_help(parser), "```"])
    for child_name, child in children:
        sections.extend(
            [
                "",
                _render_argparse_tree(
                    child,
                    heading_level + 1,
                    extra_children,
                    heading=child_name,
                    flattened_parsers=flattened_parsers,
                ),
            ]
        )
    return "\n".join(sections)


def dp3_sh_help() -> str:
    """Render the complete `dp3 sh` command tree from its argparse help."""
    root_parser = build_sh_parser()
    entity_type_parser = build_entity_type_parser("<ETYPE>")
    extra_children = {"dp3 sh entity": [entity_type_parser]}
    flattened_parsers = {
        "dp3 sh entity <ETYPE>": "",
        "dp3 sh entity <ETYPE> id": "id",
    }
    sections = ["```text", _format_parser_help(root_parser), "```"]
    for child_name, child in _subcommand_parser_items(root_parser):
        sections.extend(
            [
                "",
                _render_argparse_tree(
                    child,
                    extra_children=extra_children,
                    heading=child_name,
                    flattened_parsers=flattened_parsers,
                ),
            ]
        )
    return "\n".join(sections)


def define_env(env):
    env.macro(dp3_sh_help)

    @env.macro
    def query_cesnet_apps(label: str, query: str) -> str:
        full_query = f"{_repo_query(CESNET_APP_REPOS)} {query}"
        return _github_code_search_link(label, full_query)

    @env.macro
    def registrar_usage(label: str, registrar_call: str) -> str:
        return query_cesnet_apps(f"`{label}`", f"registrar.{registrar_call}")

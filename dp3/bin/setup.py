"""DP3 Setup Script for creating a DP3 application."""

import argparse
import contextlib
import shutil
import sys
from pathlib import Path


def replace_template(directory: Path, template: str, replace_with: str):
    """Replace all occurrences of `template` with the given text."""
    for file in directory.rglob("*"):
        if file.is_file():
            replace_template_file(file, template, replace_with)


def replace_template_file(file: Path, template: str, replace_with: str):
    with contextlib.suppress(UnicodeDecodeError, PermissionError), file.open("r+") as f:
        contents = f.read()
        contents = contents.replace(template, replace_with)
        f.seek(0)
        f.write(contents)
        f.truncate()


def init_parser(parser):
    """
    Initialize an argparse object to parse the project directory and the app name.
    """
    parser.add_argument("project_dir", help="The project directory.", type=str)
    parser.add_argument("app_name", help="The name of the application.", type=str)


def run():
    print(
        "WARNING: The `dp3-setup` entrypoint is deprecated. Please use `dp3 setup` instead. ",
        file=sys.stderr,
    )

    parser = argparse.ArgumentParser(description="Create a DP3 application.")
    init_parser(parser)
    args = parser.parse_args()

    main(args)


def main(args):
    app_name = args.app_name.lower()

    # Ensure the project directory exists.
    project_dir = Path(args.project_dir)
    project_dir = project_dir.absolute()
    project_dir.mkdir(exist_ok=True, parents=True)

    # Get the current package location.
    package_dir = Path(__file__).parent.parent

    # Copy the template files to the project directory.
    shutil.copytree(package_dir / "template" / "app", project_dir, dirs_exist_ok=True)

    replace_template(project_dir, "{{DP3_APP}}", app_name)
    replace_template(project_dir, "__dp3_app__", app_name)


if __name__ == "__main__":
    run()

"""DP3 Setup Script for creating a DP3 application."""
import argparse
import shutil
from pathlib import Path


def replace_template(directory: Path, template: str, replace_with: str):
    """Replace all occurrences of `template` with the given text."""
    for file in directory.rglob("*"):
        if file.is_file():
            try:
                with file.open("r+") as f:
                    contents = f.read()
                    contents = contents.replace(template, replace_with)
                    f.seek(0)
                    f.write(contents)
                    f.truncate()
            except UnicodeDecodeError:
                print(f"Skipping {file} as it is a binary file.")
            except PermissionError:
                print(f"Skipping {file} as it is a read-only file.")


def main():
    # Create an argparse object to parse the project directory and the app name.
    parser = argparse.ArgumentParser(description="Create a DP3 application.")
    parser.add_argument("project_dir", help="The project directory.", type=str)
    parser.add_argument("app_name", help="The name of the application.", type=str)
    args = parser.parse_args()
    app_name = args.app_name.lower()

    # Ensure the project directory exists.
    project_dir = Path(args.project_dir)
    project_dir = project_dir.absolute()
    project_dir.mkdir(exist_ok=True, parents=True)

    # Get the current package location.
    package_dir = Path(__file__).parent.parent.parent

    directories_to_copy = ["config"]
    for directory in directories_to_copy:
        shutil.copytree(package_dir / directory, project_dir / directory, dirs_exist_ok=True)

    # Copy the template files to the project directory.
    shutil.copytree(package_dir / "install" / "template", project_dir, dirs_exist_ok=True)

    replace_template(project_dir, "{{DP3_APP}}", app_name)
    replace_template(project_dir, "__dp3_app__", app_name)


if __name__ == "__main__":
    main()

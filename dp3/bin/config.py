"""DP3 Setup Config Script for creating a DP3 application."""

import shutil
import sys
from pathlib import Path

from dp3.bin.setup import replace_template, replace_template_file
from dp3.common.config import read_config_dir


def init_parser(parser):
    """
    There are two desired use-cases for this command:

    ```
    dp3 config nginx --app-name <APP_NAME> --hostname <SERVER_HOSTNAME> --www-root <DIRECTORY>
    ```

    ```
    dp3 config supervisor --app-name <APP_NAME> --config <CONFIG_DIR>
    ```
    """
    parser.add_argument(
        "subcommand",
        help="What configuration to setup. "
        "'nginx' requires appname, hostname and www_root, "
        "'supervisor' requires appname and config.",
        choices=["nginx", "supervisor"],
    )
    parser.add_argument(
        "--app-name", dest="app_name", help="The name of the application.", type=str
    )
    parser.add_argument(
        "--hostname", dest="server_hostname", help="The hostname of the server.", type=str
    )
    parser.add_argument(
        "--www-root",
        dest="www_root",
        help="The directory where served HTML content will be placed.",
        type=str,
    )
    parser.add_argument(
        "--config",
        dest="config_dir",
        help="The directory where the DP3 config of your application is stored.",
        type=str,
    )


def config_nginx(app_name, server_hostname, www_root):
    """Configure nginx to serve the application."""
    # Get the current package location.
    package_dir = Path(__file__).parent.parent

    nginx_dir = Path("/etc/nginx/")

    # Copy the template files to the project directory.
    shutil.copytree(package_dir / "template" / "nginx", nginx_dir, dirs_exist_ok=True)

    replace_template(nginx_dir, "{{DP3_APP}}", app_name)
    replace_template(nginx_dir, "__SERVER_NAME__", server_hostname)
    replace_template(nginx_dir, "__WWW_ROOT__", www_root)

    # Create the www root directory.
    www_root_dir = Path(www_root)
    www_root_dir.mkdir(exist_ok=True, parents=True)
    shutil.copytree(package_dir / "template" / "html", www_root_dir, dirs_exist_ok=True)

    replace_template(www_root_dir, "{{DP3_APP}}", app_name)
    replace_template(www_root_dir, "{{HOSTNAME}}", server_hostname)

    shutil.chown(www_root_dir, user=app_name, group=app_name)
    www_root_dir.chmod(0o775)


def get_python_directories() -> tuple[Path, Path]:
    """
    Get the directory where the DP3 executable is located and where the DP3 library is installed.

    There are two cases we need to consider:

    - The package is installed in a normal way, be it in virtual environment,
      in `~/.local` or in `/usr/local`. The path will allways look something like
      `/path/to/python/lib/python3.X/{site|dist}-packages/dp3`.
      The path to the executable will be `/path/to/python/bin/dp3`.
    - The package is installed in editable mode, in which case the path of the
      package depends on the location of the repo. The path to the executable relates
      in no way to the path of the package, we cannot be sure.
      We therefore give it our best guess, which is the path of the python executable.
      This will be correct only in the case of a virtual environment, but it is the best
      we can do.
    """
    package_path = Path(__file__).parent.parent.absolute()
    packages_path = package_path.parent
    lib_python_path = packages_path.parent

    if "-packages" in packages_path.name and "python3" in lib_python_path.name:
        binary_path = package_path.parent.parent.parent.parent / "bin"
    else:  # This is a development install.
        binary_path = Path(sys.executable).parent.absolute()
    return binary_path, package_path


def config_supervisor(app_name, config_dir):
    """
    Configure supervisor for process management.
    Replaces templates: DP3_EXE, DP3_APP, CONFIG_DIR, DP3_BIN, DP3_LIB
    """
    # Get the current package location and other relative directories.
    python_bin_dir, package_dir = get_python_directories()
    dp3_executable_path = str(python_bin_dir / "dp3")
    abs_config_dir = Path(config_dir).absolute()

    # Load configured worker count from dp3 config.
    config = read_config_dir(config_dir, recursive=True)
    worker_count = int(config.get("processing_core.worker_processes"))

    # Ensure the config directory exists.
    supervisor_dir = Path(f"/etc/{app_name}/")
    supervisor_dir.mkdir(exist_ok=True, parents=True)
    shutil.chown(supervisor_dir, user=app_name, group=app_name)
    supervisor_dir.chmod(0o775)

    # Copy the template files to the project directory.
    shutil.copytree(package_dir / "template" / "supervisor", supervisor_dir, dirs_exist_ok=True)

    replace_template(supervisor_dir, "{{DP3_APP}}", app_name)
    replace_template(supervisor_dir, "{{CONFIG_DIR}}", str(abs_config_dir))
    replace_template(supervisor_dir, "{{DP3_BIN}}", str(python_bin_dir))
    replace_template(supervisor_dir, "{{DP3_EXE}}", dp3_executable_path)
    replace_template(supervisor_dir, "{{DP3_PACKAGE_DIR}}", str(package_dir))
    replace_template(supervisor_dir, "{{WORKER_COUNT}}", str(worker_count))

    # Set up the systemd service to start supervisor.
    service_path = Path(f"/etc/systemd/system/{app_name}.service")
    shutil.copy(
        package_dir / "template" / "systemd" / "app.service",
        service_path,
    )
    replace_template_file(service_path, "{{DP3_APP}}", app_name)

    # Set up the appctl script.
    appctl_path = Path(f"/usr/bin/{app_name}ctl")
    shutil.copy(
        package_dir / "template" / "appctl",
        appctl_path,
    )
    replace_template_file(appctl_path, "{{DP3_APP}}", app_name)
    appctl_path.chmod(0o755)


def main(args):
    required_args = []
    if args.subcommand == "nginx":
        required_args = ["app_name", "server_hostname", "www_root"]
    if args.subcommand == "supervisor":
        required_args = ["app_name", "config_dir"]
    for arg in required_args:
        if not getattr(args, arg):
            print(
                f"Missing required argument: {arg}. "
                f"The {args.subcommand} requires the following arguments: {required_args}",
                file=sys.stderr,
            )
            sys.exit(1)

    if args.subcommand == "nginx":
        config_nginx(args.app_name, args.server_hostname, args.www_root)
    if args.subcommand == "supervisor":
        config_supervisor(args.app_name, args.config_dir)

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


def config_supervisor(app_name, config_dir):
    """
    Configure supervisor for process management.
    Replaces templates: DP3_EXE, DP3_APP, CONFIG_DIR, DP3_BIN, DP3_LIB
    """
    # Get the current package location and other relative directories.
    package_dir = Path(__file__).parent.parent
    python_bin_dir = Path(sys.executable).parent.absolute()
    python_lib_dir = python_bin_dir.parent / "lib"
    dp3_executable_path = str(python_bin_dir / "dp3")
    abs_config_dir = Path(config_dir).absolute()

    # Load configured worker count from dp3 config.
    config = read_config_dir(config_dir, recursive=True)
    worker_count = int(config.get("processing_core.worker_processes"))

    # Ensure the config directory exists.
    supervisor_dir = Path(f"/etc/{app_name}/")
    supervisor_dir.mkdir(exist_ok=True, parents=True)
    shutil.chown(supervisor_dir, user=app_name, group=app_name)
    supervisor_dir.chmod(0o664)

    # Ensure a log directory exists.
    log_dir = Path(f"/var/log/{app_name}/")
    log_dir.mkdir(exist_ok=True, parents=True)
    shutil.chown(log_dir, user=app_name, group=app_name)
    log_dir.chmod(0o664)

    # Copy the template files to the project directory.
    shutil.copytree(package_dir / "template" / "supervisor", supervisor_dir, dirs_exist_ok=True)

    replace_template(supervisor_dir, "{{DP3_APP}}", app_name)
    replace_template(supervisor_dir, "{{CONFIG_DIR}}", str(abs_config_dir))
    replace_template(supervisor_dir, "{{DP3_BIN}}", str(python_bin_dir))
    replace_template(supervisor_dir, "{{DP3_EXE}}", dp3_executable_path)
    replace_template(supervisor_dir, "{{DP3_LIB}}", str(python_lib_dir))
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

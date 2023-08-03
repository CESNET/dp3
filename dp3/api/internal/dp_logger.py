import logging
import pathlib

from dp3.common.datapoint import DataPointBase


class DPLogger:
    """Datapoint logger

    Logs good/bad datapoints into file for further analysis.
    They are logged in JSON format.
    Bad datapoints are logged together with their error message.

    Logging may be disabled in `api.yml` configuration file:

    ```yml
    # ...
    datapoint_logger:
      good_log: false
      bad_log: false
    # ...
    ```

    """

    LOG_FORMATTER = logging.Formatter("%(asctime)s (%(src)s) | %(message)s")
    UNKNOWN_SRC_MSG = "UNKNOWN"

    def __init__(self, config: dict):
        if not config:
            config = {}

        good_log_file = config.get("good_log", False)
        bad_log_file = config.get("bad_log", False)

        # Setup loggers
        self._good_logger = self.setup_logger("GOOD", good_log_file)
        self._bad_logger = self.setup_logger("BAD", bad_log_file)

    def setup_logger(self, name: str, log_file: str):
        """Creates new logger instance with `log_file` as target"""
        # Create log handler
        if log_file:
            parent_path = pathlib.Path(log_file).parent
            if not parent_path.exists():
                raise FileNotFoundError(
                    f"The directory {parent_path} does not exist,"
                    " check the configured path or create the directory."
                )
            log_handler = logging.FileHandler(log_file)
            log_handler.setFormatter(self.LOG_FORMATTER)
        else:
            log_handler = logging.NullHandler()

        # Get logger instance
        logger = logging.getLogger(name)
        logger.addHandler(log_handler)
        logger.setLevel(logging.INFO)

        return logger

    def log_good(self, dps: list[DataPointBase], src: str = UNKNOWN_SRC_MSG):
        """Logs good datapoints

        Datapoints are logged one-by-one in processed form.
        Source should be IP address of incomping request.
        """
        for dp in dps:
            self._good_logger.info(dp.json(), extra={"src": src})

    def log_bad(self, request_body: str, validation_error_msg: str, src: str = UNKNOWN_SRC_MSG):
        """Logs bad datapoints including the validation error message

        Whole request body is logged at once (JSON string is expected).
        Source should be IP address of incomping request.
        """
        # Remove newlines from request body
        request_body = request_body.replace("\n", " ")

        # Prepend error message with tabs
        validation_error_msg = validation_error_msg.replace("\n", "\n\t")

        self._bad_logger.info(f"{request_body}\n\t{validation_error_msg}", extra={"src": src})

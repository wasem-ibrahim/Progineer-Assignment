
import json
import logging
import logging.config
import os


class LoggerConfigurator:
    """
    Manages the logging configuration for an application. This class sets up logging based on a 
    specified configuration file, log file path, and verbosity level.

    This configuration manager allows for dynamic adjustments of logging settings and is 
    particularly useful for applications with complex logging needs.
    """

    def __init__(self, config_path: str, log_file: str, verbose: bool):
        """
        Initializes the LoggerConfigurator with the configuration file path, log file path, and verbosity flag.

        Args:
            config_path (str): Path to the logging configuration file.
            log_file (str): Path to the log file where logs will be written.
            verbose (bool): Flag to set verbose logging.
        """
        self.config_path = config_path
        self.log_file = log_file
        self.verbose = verbose

    def setup_logging(self) -> None:
        """
        Sets up logging configuration based on the provided configuration file, log file, and verbosity level.
        """
        if os.path.exists(self.config_path):
            with open(self.config_path, "r") as f:
                config = json.load(f)

                # Update log file path dynamically
                config["handlers"]["fileHandler"]["filename"] = self.log_file

                # Adjust level for console handler based on verbose flag
                console_level = "DEBUG" if self.verbose else "WARNING"
                config["handlers"]["consoleHandler"]["level"] = console_level

                # Apply the logging configuration
                logging.config.dictConfig(config)
        else:
            print(
                f"Logging configuration file '{self.config_path}' not found.")
            exit(1)

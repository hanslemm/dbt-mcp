import unittest
from unittest.mock import MagicMock, patch

from tests.mocks.config import mock_config


class TestDbtCliTools(unittest.TestCase):
    @patch("subprocess.Popen")
    def test_run_command_adds_quiet_flag_to_verbose_commands(self, mock_popen):
        # Import here to prevent circular import issues during patching
        from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools

        # Mock setup
        mock_process = MagicMock()
        mock_process.communicate.return_value = ("command output", None)
        mock_popen.return_value = mock_process

        # Create a mock FastMCP and Config
        mock_fastmcp = MagicMock()

        # Capture the registered tools
        tools = {}

        # Patch the tool decorator to capture functions
        def mock_tool_decorator(**kwargs):
            def decorator(func):
                tools[func.__name__] = func
                return func

            return decorator

        mock_fastmcp.tool = mock_tool_decorator

        # Register the tools
        register_dbt_cli_tools(mock_fastmcp, mock_config.dbt_cli_config)

        # Test each verbose command (build, compile, docs, parse, run, test)
        verbose_commands = ["build", "compile", "docs", "parse", "run", "test"]

        for command in verbose_commands:
            # Reset mock
            mock_popen.reset_mock()

            # Call the captured function
            tools[command]()

            # Check specific command formatting
            if command == "docs":
                # For docs command, check if ["docs", "generate"] gets transformed to ["docs", "--quiet", "generate"]
                args_list = mock_popen.call_args.kwargs.get("args")
                self.assertEqual(
                    args_list,
                    [
                        "/path/to/dbt",
                        "docs",
                        "--quiet",
                        "generate",
                        "--log-format",
                        "json",
                    ],
                )
            else:
                # Check if the --quiet flag was added
                args_list = mock_popen.call_args.kwargs.get("args")
                self.assertEqual(
                    args_list[:3],
                    ["/path/to/dbt", command, "--quiet"],
                )
                # check that the log format is last
                self.assertEqual(
                    args_list[-2:],
                    ["--log-format", "json"],
                )

    @patch("subprocess.Popen")
    def test_non_verbose_commands_not_modified(self, mock_popen):
        # Import here to prevent circular import issues during patching
        from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools

        # Mock setup
        mock_process = MagicMock()
        mock_process.communicate.return_value = ("command output", None)
        mock_popen.return_value = mock_process

        # Create a mock FastMCP and Config
        mock_fastmcp = MagicMock()

        # Capture the registered tools
        tools = {}

        # Patch the tool decorator to capture functions
        def mock_tool_decorator(**kwargs):
            def decorator(func):
                tools[func.__name__] = func
                return func

            return decorator

        mock_fastmcp.tool = mock_tool_decorator

        # Register the tools
        register_dbt_cli_tools(mock_fastmcp, mock_config.dbt_cli_config)

        # Test "list" (non-verbose) command
        mock_popen.reset_mock()
        tools["ls"]()

        # Check that --quiet flag was NOT added
        args_list = mock_popen.call_args.kwargs.get("args")
        self.assertEqual(args_list[:2], ["/path/to/dbt", "list"])
        self.assertEqual(args_list[-2:], ["--log-format", "json"])

    @patch("subprocess.Popen")
    def test_show_command_correctly_formatted(self, mock_popen):
        # Import here to prevent circular import issues during patching
        from dbt_mcp.dbt_cli.tools import register_dbt_cli_tools

        # Mock setup
        mock_process = MagicMock()
        mock_process.communicate.return_value = ("command output", None)
        mock_popen.return_value = mock_process

        # Create a mock FastMCP and Config
        mock_fastmcp = MagicMock()

        # Capture the registered tools
        tools = {}

        # Patch the tool decorator to capture functions
        def mock_tool_decorator(**kwargs):
            def decorator(func):
                tools[func.__name__] = func
                return func

            return decorator

        mock_fastmcp.tool = mock_tool_decorator

        # Register the tools
        register_dbt_cli_tools(mock_fastmcp, mock_config.dbt_cli_config)

        # Test show command with and without limit
        mock_popen.reset_mock()
        tools["show"]("SELECT * FROM my_model")

        # Check command formatting without limit
        args_list = mock_popen.call_args.kwargs.get("args")
        self.assertEqual(
            args_list,
            [
                "/path/to/dbt",
                "show",
                "--inline",
                "SELECT * FROM my_model",
                "--favor-state",
                "--output",
                "json",
                "--log-format",
                "json",
            ],
        )

        # Reset mock and test with limit
        mock_popen.reset_mock()
        tools["show"]("SELECT * FROM my_model", limit=10)

        # Check command formatting with limit
        args_list = mock_popen.call_args.kwargs.get("args")
        self.assertEqual(
            args_list,
            [
                "/path/to/dbt",
                "show",
                "--inline",
                "SELECT * FROM my_model",
                "--favor-state",
                "--limit",
                "10",
                "--output",
                "json",
                "--log-format",
                "json",
            ],
        )


if __name__ == "__main__":
    unittest.main()

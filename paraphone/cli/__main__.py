import argparse
import logging
from builtins import hasattr
from pathlib import Path

from paraphone.paraphone.cli.commands import WorkspaceInitCommand, ImportCommand
from ..utils import stream_handler

argparser = argparse.ArgumentParser("paraphone")
argparser.add_argument("workspace_path", type=Path, help="Path to workspace")
argparser.add_argument("-v", "--verbose", action="store_true",
                       help="Show debug information in the standard output")
subparsers = argparser.add_subparsers()

commands = [WorkspaceInitCommand, ImportCommand]

for command in commands:
    subparser = subparsers.add_parser(command.COMMAND)
    subparser.set_defaults(func=command.main)
    command.init_parser(subparser)


if __name__ == '__main__':
    args = argparser.parse_args()
    stream_handler.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    if hasattr(args, "func"):
        args.func(args)
    else:
        argparser.print_help()
'''
CLI Entry Point
'''
import argparse
import logging
import os

from omnilake.client.client import __version__

from omnilake.client.commands import __all__ as available_commands


def _execute_command(args):
    """
    Execute the command requested by the user.
    """
    command_name = args.command

    if not command_name:
        print('no command provided')

        return

    if command_name not in available_commands:
        raise ValueError(f'Command {command_name} not found')
    
    command = available_commands[command_name]()

    command.run(args)


def main():
    parser = argparse.ArgumentParser(description='OmniLake CLI')

    parser.add_argument('--base_dir', '-D', help='Base Directory to work off index', default=os.getcwd())

    parser.add_argument('--version', '-v', action='version', version=__version__)

    verbose = parser.add_mutually_exclusive_group()

    verbose.add_argument('-V', dest='loglevel', action='store_const',
                         const=logging.INFO,
                         help='Set log-level to INFO.')

    verbose.add_argument('-VV', dest='loglevel', action='store_const',
                         const=logging.DEBUG,
                         help='Set log-level to DEBUG.')

    parser.set_defaults(loglevel=logging.WARNING)

    subparsers = parser.add_subparsers(dest='command')

    for _, command_class in available_commands.items():
        command_class.configure_parser(subparsers)

    args = parser.parse_args()

    logging.basicConfig(level=args.loglevel)

    _execute_command(args)
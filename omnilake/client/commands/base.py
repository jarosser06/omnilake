from argparse import ArgumentParser


class Command:
    command_name = None

    description = None

    @classmethod
    def configure_parser(cls, parser: ArgumentParser) -> ArgumentParser:
        # Override this to add arguments to the command
        return parser

    def run(self, args):
        raise NotImplementedError
from argparse import ArgumentParser, Namespace


class BaseTask:
    cmd_name: str = None

    def init_parser(self, parser: ArgumentParser):
        pass

    def run(self, args: Namespace):
        pass

    # TODO : remember about logging
import logging
import sys
from argparse import ArgumentParser, Namespace
from pathlib import Path

from ..tasks.base import BaseTask
from ..tasks.dictionaries import CMUFRSetupTask, LexiqueSetupTask, INSEESetupTask, CMUENSetupTask, CelexSetupTask, \
    PhonemizerSetupTask
from ..tasks.imports import DatasetImportTask, FamiliesImportTask
from ..tasks.tokenize import TokenizeFrenchTask, TokenizeEnglishTask
from ..tasks.workspace_init import WorkspaceInitTask
from ..utils import setup_file_handler
from ..workspace import Workspace


class BaseCommand:
    COMMAND = "command"
    DESCRIPTION = "Command description"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        pass

    @classmethod
    def run_task(cls, task: BaseTask, workspace: Workspace):
        task.check_requirements(workspace)
        task.run(workspace)
        task.check_output(workspace)

    @classmethod
    def run(cls, args: Namespace, workspace: Workspace):
        pass

    @classmethod
    def main(cls, args: Namespace):
        workspace = Workspace(args.workspace_path)
        # setting up file logger to log in the right folder from the workspace
        setup_file_handler(cls.COMMAND, workspace.logs / Path("outputs"))
        cls.run(args, workspace)
        # logging command in commands.log file
        with open(workspace.logs / Path("commands.log"), "a") as cmds_file:
            cmds_file.write(" ".join(sys.argv[1:]))
            cmds_file.write("\n")


class WorkspaceInitCommand(BaseCommand):
    COMMAND = "init"
    DESCRIPTION = "Initialize workspace directory"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument("-l", "--lang", type=str, default="fr",
                            choices=["en", "fr"],
                            help="Language for the fake words generation project")

    @classmethod
    def run(cls, args: Namespace, workspace: Workspace):
        task = WorkspaceInitTask(args.lang)
        cls.run_task(task, workspace)


class ImportDatasetCommand(BaseCommand):
    COMMAND = "dataset"
    DESCRIPTION = "Import a dataset"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument("dataset_path", type=Path,
                            help="Path to the root of the dataset")
        parser.add_argument("--type", required=True,
                            choices=["littaudio", "librivox"],
                            help="Dataset type to import")
        import_style = parser.add_mutually_exclusive_group()
        import_style.add_argument("--copy", action="store_true")
        import_style.add_argument("--symlink", action="store_true")

    @classmethod
    def run(cls, args: Namespace, workspace: Workspace):
        task = DatasetImportTask(
            dataset_path=args.dataset_path,
            dataset_type=args.type,
            copy=args.copy,
            symlink=args.symlink,
        )
        cls.run_task(task, workspace)


class ImportFamiliesCommand(BaseCommand):
    COMMAND = "families"
    DESCRIPTION = "Import families"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument("families_path",
                            help="Path to the families description file (most likely matche2.csv)")

    @classmethod
    def run(cls, args: Namespace, workspace: Workspace):
        task = FamiliesImportTask(args.families_path)
        cls.run_task(task, workspace)


class ImportCommand(BaseCommand):
    COMMAND = "import"
    DESCRIPTION = "Import some data to the workspace"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        subparsers = parser.add_subparsers()
        for command in [ImportFamiliesCommand, ImportDatasetCommand]:
            subparser = subparsers.add_parser(command.COMMAND)
            subparser.set_defaults(func=command.main)
            command.init_parser(subparser)


class SetupDictionnaryCommand(BaseCommand):
    COMMAND = "dict-setup"
    DESCRIPTION = "Setup dictionnaries"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument("--celex_path", type=Path,
                            help="Path to the root of the CELEX2 dictionnary "
                                 "dataset.")

    @classmethod
    def run(cls, args: Namespace, workspace: Workspace):
        lang = workspace.config["lang"]
        if lang == "fr":
            setup_tasks = [CMUFRSetupTask(), LexiqueSetupTask(),
                           INSEESetupTask(), PhonemizerSetupTask()]
        else:
            if args.celex_path is None:
                logging.error("A path to the celex dataset has to be provided "
                              "via the --celex_path argument")
                return

            setup_tasks = [CMUENSetupTask(), CelexSetupTask(args.celex_path), PhonemizerSetupTask()]

        for task in setup_tasks:
            cls.run_task(task, workspace)


class TokenizeCommand(BaseCommand):
    COMMAND = "tokenize"
    DESCRIPTION = "Tokenize all texts using the dictionnaries"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        pass

    @classmethod
    def run(cls, args: Namespace, workspace: Workspace):
        lang = workspace.config["lang"]
        if lang == "fr":
            task = TokenizeFrenchTask()
        else:
            task = TokenizeEnglishTask()
        cls.run_task(task, workspace)

import logging
import sys
from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import List, Type, Union

from ..tasks.base import BaseTask
from ..tasks.corpora import CorporaCreationTask
from ..tasks.dictionaries import CMUFRSetupTask, LexiqueSetupTask, INSEESetupTask, CMUENSetupTask, CelexSetupTask, \
    PhonemizerSetupTask
from ..tasks.imports import DatasetImportTask, FamiliesImportTask
from ..tasks.phonemize import PhonemizeFrenchTask, PhonemizeEnglishTask
from ..tasks.tokenize import TokenizeFrenchTask, TokenizeEnglishTask
from ..tasks.workspace_init import WorkspaceInitTask
from ..utils import setup_file_handler, logger
from ..workspace import Workspace


class BaseCommand:
    COMMAND = "command"
    DESCRIPTION = "Command description"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        pass

    @classmethod
    def run_task(cls, task: BaseTask, workspace: Workspace):
        logging.debug(f"Checking requirements for task {task.__class__.__name__}")
        task.check_requirements(workspace)
        logger.info(f"Running task {task.__class__.__name__}")
        task.run(workspace)
        logging.debug(f"Checking output for task {task.__class__.__name__}")
        task.check_output(workspace)

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) \
            -> Union[BaseTask, List[BaseTask]]:
        """To be overloaded by child classes. Called by `main`"""
        pass

    @classmethod
    def main(cls, args: Namespace):
        workspace = Workspace(args.workspace_path)
        # setting up file logger to log in the right folder from the workspace
        setup_file_handler(cls.COMMAND, workspace.logs / Path("outputs"))
        tasks = cls.build_task(args, workspace)
        if isinstance(tasks, BaseTask):
            tasks = [tasks]
        for task in tasks:
            cls.run_task(task, workspace)
        # logging command in commands.log file
        with open(workspace.logs / Path("commands.log"), "a") as cmds_file:
            cmds_file.write(" ".join(sys.argv[1:]))
            cmds_file.write("\n")


class CommandGroup(BaseCommand):
    SUBCOMMANDS: List[Type[BaseCommand]]

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        subparsers = parser.add_subparsers()
        for command in cls.SUBCOMMANDS:
            subparser = subparsers.add_parser(command.COMMAND)
            subparser.set_defaults(func=command.main)
            command.init_parser(subparser)


class WorkspaceInitCommand(BaseCommand):
    COMMAND = "init"
    DESCRIPTION = "Initialize workspace directory"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument("-l", "--lang", type=str, default="fr",
                            choices=["en", "fr"],
                            help="Language for the fake words generation project")

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) \
            -> Union[BaseTask, List[BaseTask]]:
        return WorkspaceInitTask(args.lang)


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
    def build_task(cls, args: Namespace, workspace: Workspace)\
            -> Union[BaseTask, List[BaseTask]]:
        return DatasetImportTask(
            dataset_path=args.dataset_path,
            dataset_type=args.type,
            copy=args.copy,
            symlink=args.symlink,
        )


class ImportFamiliesCommand(BaseCommand):
    COMMAND = "families"
    DESCRIPTION = "Import families"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument("families_path",
                            help="Path to the families description file (most likely matche2.csv)")

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace)\
            -> Union[BaseTask, List[BaseTask]]:
        return FamiliesImportTask(args.families_path)


class ImportCommand(CommandGroup):
    COMMAND = "import"
    DESCRIPTION = "Import some data to the workspace"
    SUBCOMMANDS = [ImportFamiliesCommand, ImportDatasetCommand]


class SetupDictionnaryCommand(BaseCommand):
    COMMAND = "dict-setup"
    DESCRIPTION = "Setup dictionnaries"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument("--celex_path", type=Path,
                            help="Path to the root of the CELEX2 dictionnary "
                                 "dataset.")

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace)\
            -> Union[BaseTask, List[BaseTask]]:
        lang = workspace.config["lang"]
        if lang == "fr":
            return [CMUFRSetupTask(), LexiqueSetupTask(),
                           INSEESetupTask(), PhonemizerSetupTask()]
        else:
            if args.celex_path is None:
                logging.error("A path to the celex dataset has to be provided "
                              "via the --celex_path argument")
                return []

            return [CMUENSetupTask(), CelexSetupTask(args.celex_path), PhonemizerSetupTask()]



class TokenizeCommand(BaseCommand):
    COMMAND = "tokenize"
    DESCRIPTION = "Tokenize all texts using the dictionaries"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        pass

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace):
        lang = workspace.config["lang"]
        if lang == "fr":
            return TokenizeFrenchTask()
        else:
            return TokenizeEnglishTask()


class CorpusGenCommand(BaseCommand):
    COMMAND = "generate"
    DESCRIPTION = "Generate words list for corpora"


    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace):
        return CorporaCreationTask()


class CorpusSynthCommand(BaseCommand):
    COMMAND = "synth"
    DESCRIPTION = "Generate audio recordings for corpora"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        pass

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace):
        pass


class CorporaCommand(CommandGroup):
    COMMAND = "corpora"
    DESCRIPTION = "Operations on corpora"
    SUBCOMMANDS = [CorpusGenCommand, CorpusSynthCommand]


class PhonemizeCommand(BaseCommand):
    COMMAND = "phonemize"
    DESCRIPTION = "Phonemize the tokenized dataset"
    
    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace):
        lang = workspace.config["lang"]
        if lang == "fr":
            return PhonemizeFrenchTask()
        else:
            return PhonemizeEnglishTask()

import sys
from argparse import ArgumentParser, Namespace
from os import cpu_count
from pathlib import Path
from typing import List, Type, Union

from ..tasks.base import BaseTask
from ..tasks.corpora import CorporaCreationTask
from ..tasks.dictionaries import CMUFRSetupTask, LexiqueSetupTask, INSEESetupTask, CMUENSetupTask, CelexSetupTask, \
    PhonemizerSetupTask
from ..tasks.filters.simple import InitFilteringTask, RandomFilterTask, RandomPairFilterTask, EqualsFilterTask, \
    LevenshteinFilterTask, MostFrequentHomophoneFilterTask
from ..tasks.filters.ngrams import NgramScoringTask, NgramBalanceScoresTask
from ..tasks.filters.seq2seq import P2GWordsFilterTask, G2PtoP2GNonWordsFilterTask
from ..tasks.imports import DatasetImportTask, FamiliesImportTask, ImportGoogleSpeakCredentials
from ..tasks.phonemize import PhonemizeFrenchTask, PhonemizeEnglishTask
from ..tasks.stats import CorporaNgramStatsTask
from ..tasks.syllabify import SyllabifyFrenchTask, SyllabifyEnglishTask
from ..tasks.tokenize import TokenizeFrenchTask, TokenizeEnglishTask
from ..tasks.workspace_init import WorkspaceInitTask
from ..tasks.wuggy_gen import WuggyPrepareTask, WuggyGenerationFrTask, WuggyGenerationEnTask
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
        logger.debug(f"Checking requirements for task {task.__class__.__name__}")
        task.check_requirements(workspace)
        logger.info(f"Running task {task.__class__.__name__}")
        task.run(workspace)
        logger.debug(f"Checking output for task {task.__class__.__name__}")
        task.check_output(workspace)
        if task.stats:
            logger.debug(f"Writing stats for task {task.__class__.__name__}")
            task.write_stats(workspace)

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
    def build_task(cls, args: Namespace, workspace: Workspace) \
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
    def build_task(cls, args: Namespace, workspace: Workspace) \
            -> Union[BaseTask, List[BaseTask]]:
        return FamiliesImportTask(args.families_path)


class ImportGoogleTTSCredentialCommand(BaseCommand):
    COMMAND = "tts-credentials"
    DESCRIPTION = "Import the google TTS credentials JSON"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument("json_path", type=Path,
                            help="Path to the JSON credentials file.")

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) -> Union[BaseTask, List[BaseTask]]:
        return ImportGoogleSpeakCredentials(args.json_path)


class ImportCommand(CommandGroup):
    COMMAND = "import"
    DESCRIPTION = "Import some data to the workspace"
    SUBCOMMANDS = [ImportFamiliesCommand, ImportDatasetCommand, ImportGoogleTTSCredentialCommand]


class SetupDictionnaryCommand(BaseCommand):
    COMMAND = "dict-setup"
    DESCRIPTION = "Setup dictionnaries"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument("--celex_path", type=Path,
                            help="Path to the root of the CELEX2 dictionnary "
                                 "dataset.")

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) \
            -> Union[BaseTask, List[BaseTask]]:
        lang = workspace.config["lang"]
        if lang == "fr":
            return [CMUFRSetupTask(), LexiqueSetupTask(),
                    INSEESetupTask(), PhonemizerSetupTask()]
        else:
            if args.celex_path is None:
                logger.error("A path to the celex dataset has to be provided "
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


class SyllabifyCommand(BaseCommand):
    COMMAND = "syllabify"
    DESCRIPTION = "Syllabify the phonetic forms of the words"

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace):
        lang = workspace.config["lang"]
        if lang == "fr":
            return SyllabifyFrenchTask()
        else:
            return SyllabifyEnglishTask()


class WuggyCommand(BaseCommand):
    COMMAND = "wuggy"
    DESCRIPTION = "Prepare dataset for wuggy and run fake word generation"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument('--num-workers', '-w', default=cpu_count(),
                            help='number of parallel workers', type=int)
        parser.add_argument('--num-candidates', '-n', default=10, type=int,
                            help='maximum number of nonword candidates per word '
                                 '(for some words, the number of candidates '
                                 'can be less than NUM_CANDIDATES)')
        parser.add_argument('--high-overlap', action="store_true",
                            help='if set, only allows overlap rate of the form (n-1)/n.'
                                 ' Slows down dramatically computations if set.')

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) -> Union[BaseTask, List[BaseTask]]:
        tasks = [WuggyPrepareTask()]
        lang = workspace.config["lang"]
        kwargs = {'num_candidates': args.num_candidates,
                  'high_overlap': args.high_overlap,
                  'num_workers': args.num_workers}
        if lang == "fr":
            tasks.append(WuggyGenerationFrTask(**kwargs))
        else:
            tasks.append(WuggyGenerationEnTask(**kwargs))

        return tasks


class FilterInitCommand(BaseCommand):
    COMMAND = "init"
    DESCRIPTION = "Initialize filtering"

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) -> Union[BaseTask, List[BaseTask]]:
        return InitFilteringTask()


class FilterRandomCommand(BaseCommand):
    COMMAND = "random"
    DESCRIPTION = "Filter out a random part of the candidates"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument("-r", "--ratio", default=0.2, type=float)

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) -> Union[BaseTask, List[BaseTask]]:
        return RandomFilterTask(args.ratio)


class FilterRandomPairsCommand(BaseCommand):
    COMMAND = "random-pairs"
    DESCRIPTION = "Keep, for each real word, only one random word/nonword pair"

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) -> Union[BaseTask, List[BaseTask]]:
        return RandomPairFilterTask()


class FilterLevenshteinCommand(BaseCommand):
    COMMAND = "levenshtein"
    DESCRIPTION = "Keep only pairs that have a phonetic levenshtein distance " \
                  "lower than the set threshold"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument("-th", "--threshold", default=2, type=int)

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) -> Union[BaseTask, List[BaseTask]]:
        return LevenshteinFilterTask(args.threshold)


class FilterEqualsCommand(BaseCommand):
    COMMAND = "equals"
    DESCRIPTION = "Filter out pairs that have the same phonetic form"

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) -> Union[BaseTask, List[BaseTask]]:
        return EqualsFilterTask()


class FilterHomophonesCommand(BaseCommand):
    COMMAND = "homophones"
    DESCRIPTION = "Filter out homophones based on frequency"

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) -> Union[BaseTask, List[BaseTask]]:
        return MostFrequentHomophoneFilterTask()


class FilterNgramCommand(BaseCommand):
    COMMAND = "ngram"
    DESCRIPTION = "Filter out fake words candidates using Ngrams statistics"

    @classmethod
    def init_parser(cls, parser: ArgumentParser):
        parser.add_argument("-c", "--corpus", type=int, help="Only run for a given corpus")

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) -> Union[BaseTask, List[BaseTask]]:
        return [NgramScoringTask(), NgramBalanceScoresTask(args.corpus)]


class FilterP2GCommand(BaseCommand):
    COMMAND = "seq2seq-words"
    DESCRIPTION = "Filter out real words using ses2seq"

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) -> Union[BaseTask, List[BaseTask]]:
        return P2GWordsFilterTask()


class FilterP2GtoG2PCommand(BaseCommand):
    COMMAND = "seq2seq-fake-words"
    DESCRIPTION = "Filter out fake words using ses2seq"

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) -> Union[BaseTask, List[BaseTask]]:
        return G2PtoP2GNonWordsFilterTask()


class FilterCommand(CommandGroup):
    COMMAND = "filter"
    DESCRIPTION = "Filter out real words/fake words candidates pair using various methods"
    SUBCOMMANDS = [FilterNgramCommand, FilterP2GCommand, FilterP2GtoG2PCommand,
                   FilterRandomCommand, FilterInitCommand, FilterEqualsCommand,
                   FilterRandomPairsCommand, FilterLevenshteinCommand,
                   FilterHomophonesCommand]


class CorporaNgramStats(BaseCommand):
    COMMAND = "corpora-ngram"
    DESCRIPTION = "Compute Ngrams for each corpus"

    @classmethod
    def build_task(cls, args: Namespace, workspace: Workspace) -> Union[BaseTask, List[BaseTask]]:
        return CorporaNgramStatsTask()


class StatsCommand(CommandGroup):
    COMMAND = "stats"
    DESCRIPTION = "Statistics on the pipeline's steps"
    SUBCOMMANDS = [CorporaNgramStats]

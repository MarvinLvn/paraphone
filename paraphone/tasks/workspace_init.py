from pathlib import Path

from yaml import safe_dump

from .base import BaseTask
from ..workspace import Workspace


class WorkspaceInitTask(BaseTask):
    creates = ["logs/",
               "logs/commands.log",
               "logs/outputs/",
               "datasets/"
               "config.yml"]

    def __init__(self, language: str = "fr"):
        super().__init__()
        self.lang = language

    def run(self, workspace: Workspace):
        # first, creating root
        workspace.root_path.mkdir(parents=True, exist_ok=True)
        # then, logs and datasets folder
        (workspace.root_path / Path("logs/outputs/")).mkdir(parents=True,
                                                            exist_ok=True)
        (workspace.root_path / Path("logs/commands.log")).touch(exist_ok=True)
        (workspace.root_path / Path("datasets/")).mkdir(exist_ok=True)
        # then, init'ing config
        config = {"lang": self.lang}
        with open(workspace.root_path / Path("config.yml"), "w") as cfg_file:
            safe_dump(config, cfg_file)



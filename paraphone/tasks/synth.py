from pathlib import Path
from typing import Optional

from .base import BaseTask
from ..workspace import Workspace


class SynthAudioTast(BaseTask):

    def __init__(self, json_token_path: Optional[Path]):
        super().__init__()

    def run(self, workspace: Workspace):
        pass
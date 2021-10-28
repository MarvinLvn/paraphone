from .base import BaseTask
from ..workspace import Workspace


class WuggyGenerationtask(BaseTask):
    requires = [
        "phonemized/"
    ]
    # TODO: probably requires stats
    creates = []

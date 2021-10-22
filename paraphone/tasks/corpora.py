from .base import BaseTask


class CorporaCreationTask(BaseTask):
    requires = [
        "datasets/index.csv",
        "datasets/tokenized/*.csv",
        "datasets/families/*.csv" # TODO : figure out families
    ]

    creates = [
        "corpora/",
        "corpora/{corpus_id}.csv"
    ]

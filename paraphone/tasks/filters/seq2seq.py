from ..base import BaseTask

# TODO : file names for the filtering steps are named
#  step[x]_[operation].csv #

class P2GTrain(BaseTask):
    pass

# TODO: for these tasks, make bash calls to the seq2seq command
class Seq2SeqTrainTask(BaseTask):
    pass


class G2PtoP2GTrainTask(BaseTask):
    pass


class G2PWordsFilterTask(BaseTask):
    pass


class G2PtoP2GNonWordsFilterTask(BaseTask):
    pass

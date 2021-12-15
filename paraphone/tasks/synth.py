import asyncio
from asyncio import Semaphore
from pathlib import Path
from typing import Optional, Iterable, List, Tuple, Awaitable, Dict, Set

from aiolimiter import AsyncLimiter
from google.cloud import texttospeech
from google.cloud.texttospeech_v1 import SynthesisInput
from tqdm import tqdm
from tqdm.asyncio import tqdm as async_tqdm

from .base import BaseTask, CorporaTaskMixin
from .filters.base import CandidatesPairCSV
from ..utils import logger
from ..workspace import Workspace

VOICES = {
    "fr": [
        "fr-FR-Wavenet-A",
        "fr-FR-Wavenet-B",
        "fr-FR-Wavenet-C",
        "fr-FR-Wavenet-D",
    ],
    "en": [
        "en-US-Wavenet-A",
        "en-US-Wavenet-B",
        "en-US-Wavenet-C",
        "en-US-Wavenet-F",
    ]
}


class GoogleSpeakSynthesizer:
    SSML_TEMPLATE = """<speak><phoneme alphabet="ipa" ph="{phonemes}"></phoneme></speak>"""
    STANDARD_VOICE_PRICE_PER_CHAR = 0.000004
    WAVENET_VOICE_PRICE_PER_CHAR = 0.000016
    NUMBER_RETRIES = 4
    RETRY_WAIT_TIME = 0.5

    def __init__(self, lang: str, voice_id: str, credentials_path: Path):
        self.lang = "en-US" if lang == "en" else "fr-FR"
        self.credentials_file = credentials_path
        self.voice_id = voice_id
        self.voice = texttospeech.VoiceSelectionParams(
            language_code=self.lang,
            name=self.voice_id,
        )
        self.audio_config = texttospeech.AudioConfig(
            audio_encoding=texttospeech.AudioEncoding.OGG_OPUS
        )
        self.client = texttospeech.TextToSpeechAsyncClient.from_service_account_file(str(credentials_path))

    def estimate_price(self, words_pho: Iterable[str], for_pho: bool = False):
        if for_pho:
            ssml_template_len = len(self.SSML_TEMPLATE.format(phonemes=""))
            all_words_length = sum(len(w) for w in words_pho)
            return (ssml_template_len + all_words_length) * self.WAVENET_VOICE_PRICE_PER_CHAR
        else:
            return sum(len(word) for word in words_pho) * self.WAVENET_VOICE_PRICE_PER_CHAR

    async def _synth_worker(self, synth_input: SynthesisInput) -> Optional[bytes]:
        for _ in range(self.NUMBER_RETRIES):
            try:
                response = await self.client.synthesize_speech(
                    input=synth_input,
                    voice=self.voice,
                    audio_config=self.audio_config
                )
            except Exception:
                logger.debug(f"Error in synth, retrying in {self.RETRY_WAIT_TIME}")
                await asyncio.sleep(self.RETRY_WAIT_TIME)
                continue
            else:
                return response.audio_content
        else:
            return None

    async def synth_ssml(self, ssml: str) -> bytes:
        response = await self._synth_worker(texttospeech.SynthesisInput(ssml=ssml))
        return response

    async def synth_phonemes(self, phonemes: List[str]) -> Tuple[bytes, List[str]]:
        ssml = self.SSML_TEMPLATE.format(phonemes="".join(phonemes))
        response = await self.synth_ssml(ssml)
        return response, phonemes

    async def synth_text(self, text: str) -> bytes:
        response = await self._synth_worker(texttospeech.SynthesisInput(text=text))
        return response

    async def synth_word(self, word: str) -> Tuple[bytes, str]:
        response = await self.synth_text(word)
        return response, word


class BaseSpeechSynthesisTask(BaseTask, CorporaTaskMixin):
    requires = [
        "synth/credentials.json",
        "corpora/wuggy_pairs/*.csv"
    ]
    MAX_REQUEST_PER_MINUTE = 500
    MAX_REQUEST_PER_SECOND = 12
    MAX_CONCURRENT_REQUEST = 10

    def __init__(self):
        super().__init__()
        # self.rate_limiter = AsyncLimiter(self.MAX_REQUEST_PER_MINUTE)
        self.rate_limiter = AsyncLimiter(self.MAX_REQUEST_PER_SECOND, time_period=1)
        self.semaphore = Semaphore(self.MAX_CONCURRENT_REQUEST)

    def store_output(self, audio_bytes: bytes, phonemic_form: str, folder: Path):
        raise NotImplemented()

    async def tasks_limiter(self, task: Awaitable[Tuple[bytes, List[str]]]):
        async with self.rate_limiter:
            async with self.semaphore:
                return await task

    async def run_pho_synth(self, words_pho: List[str],
                            synthesizer: GoogleSpeakSynthesizer,
                            output_folder: Path):
        synth_tasks = [self.tasks_limiter(synthesizer.synth_phonemes(word_pho.split(" ")))
                       for word_pho in words_pho]
        for synth_task in async_tqdm.as_completed(synth_tasks):
            audio_bytes, phonemes = await synth_task
            if audio_bytes is None:
                logger.warning(f"Got none bytes for {phonemes}, skipping")
                continue
            self.store_output(audio_bytes, " ".join(phonemes), output_folder)

    async def run_word_synth(self, words: List[str],
                             synthesizer: GoogleSpeakSynthesizer,
                             output_folder: Path):
        synth_tasks = [self.tasks_limiter(synthesizer.synth_word(word))
                       for word in words]
        for synth_task in async_tqdm.as_completed(synth_tasks):
            audio_bytes, word = await synth_task
            if audio_bytes is None:
                logger.warning(f"Got none bytes for {word}, skipping")
                continue
            self.store_output(audio_bytes, word, output_folder)


class TestSynthesisTask(BaseSpeechSynthesisTask):
    creates = [
        "synth/tests/*.ogg"
    ]

    def __init__(self):
        super().__init__()
        self.test_words: Dict[str, str] = dict()

    def store_output(self, audio_bytes: bytes, phonemic_form: str, folder: Path):
        pho = self.test_words[phonemic_form]
        with open(folder / Path(f"{pho}-{phonemic_form}.ogg"), "wb") as file:
            file.write(audio_bytes)

    def run(self, workspace: Workspace):
        credentials_path = workspace.synth / Path("credentials.json")
        wuggy_pairs_folder = workspace.corpora / Path("wuggy_pairs/")

        logger.info("Gathering all the phonemes used in the wuggy pairs..")

        for corpus_pairs_path in wuggy_pairs_folder.iterdir():
            for _, word_pho, _ in CandidatesPairCSV(corpus_pairs_path):
                word_pho_set = set(word_pho.split(" "))
                # finding out if there's a new phoneme candidate, and if so,
                # using the current word as an example
                new_pho_candidates = word_pho_set - set(self.test_words.values())
                if new_pho_candidates:
                    self.test_words[word_pho] = new_pho_candidates.pop()

        logger.info(f"Found test words for {len(self.test_words)} phonemes")
        logger.debug("Test words: ")
        for test_word_pho, pho in self.test_words.items():
            logger.debug(f"\t {pho}: {test_word_pho}")

        logger.info("Synthesizing test words...")
        test_audio_folder = workspace.synth / Path("tests/")
        test_audio_folder.mkdir(parents=True, exist_ok=True)
        lang = workspace.config["lang"]
        synth = GoogleSpeakSynthesizer(lang=lang, voice_id=VOICES[lang][0],
                                       credentials_path=credentials_path)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run_pho_synth(list(self.test_words.keys()),
                                                   synth,
                                                   test_audio_folder))


class BaseCorporaSynthesisTask(BaseSpeechSynthesisTask):
    creates = [
        "synth/audio/*/*.mp3"
    ]
    SYNTH_SUBFOLDER: str
    FOR_PHONETIC: bool

    def __init__(self, no_confirmation: bool = False,
                 for_corpus: Optional[int] = None):
        super().__init__()
        self.for_corpus = for_corpus
        self.no_confirmation = no_confirmation

    @staticmethod
    def get_filename(phonemic_form: str):
        raise NotImplemented()

    def store_output(self, audio_bytes: bytes, word: str, folder: Path):
        with open(folder / Path(self.get_filename(word)), "wb") as file:
            file.write(audio_bytes)

    def find_corpora(self, workspace: Workspace) -> List[Tuple[int, Path]]:
        corpora = super().find_corpora(workspace.corpora / Path("wuggy_pairs/"))
        if self.for_corpus is not None:
            for corpus_id, corpus_path in corpora:
                if corpus_id == self.for_corpus:
                    corpora = [(corpus_id, corpus_path)]
                    break
            else:
                raise ValueError(f"Couldn't find data for corpus {self.for_corpus}")

        return corpora

    def init_synthesizers(self, workspace: Workspace) -> List[GoogleSpeakSynthesizer]:
        credentials_path = workspace.synth / Path("credentials.json")
        lang = workspace.config["lang"]
        voices = VOICES[lang]
        logger.info(f"Using voices {', '.join(voices)} for synthesis.")
        return [GoogleSpeakSynthesizer(lang, voice_id, credentials_path)
                for voice_id in voices]

    def get_words(self, corpus_path: Path) -> Set[str]:
        raise NotImplemented()

    def run(self, workspace: Workspace):
        corpora = self.find_corpora(workspace)
        synth_folder = workspace.synth / Path(f"audio/{self.SYNTH_SUBFOLDER}/")

        logger.info("Parsing corpus words...")
        # either actual words or phonetic forms
        all_words = set()
        for corpus_id, corpus_path in tqdm(corpora):
            all_words.update(self.get_words(corpus_path))

        logger.info(f"Found {len(all_words)} words to synthesize")

        synthesizers = self.init_synthesizers(workspace)
        synth_words = {
            synth: list(all_words) for synth in synthesizers
        }
        # filtering words that might already have been generated
        for synth, words in list(synth_words.items()):
            audio_folder = synth_folder / Path(synth.voice_id)
            if not audio_folder.exists():
                continue
            elif not list(audio_folder.iterdir()):
                continue
            synth_words[synth] = [
                word for word in words
                if not (audio_folder / Path(self.get_filename(word))).exists()
            ]
            logger.info(f"{len(words) - len(synth_words[synth])} words "
                        f"already exist for {synth.voice_id} and won't be synthesized")

        total_cost = sum(synth.estimate_price(words, for_pho=self.FOR_PHONETIC)
                         for synth, words in synth_words.items())
        logger.info(f"Estimated cost is {total_cost}$")
        if not self.no_confirmation:
            if input("Do you want to proceed?\n[Y/n]").lower() != "y":
                logger.info("Aborting")
                return

        logger.info("Starting synthesis...")
        loop = asyncio.get_event_loop()
        for synth, words in synth_words.items():
            logger.info(f"For synth with voice id {synth.voice_id}")
            audio_folder = synth_folder / Path(synth.voice_id)
            audio_folder.mkdir(parents=True, exist_ok=True)
            if self.FOR_PHONETIC:
                async_tasks = self.run_pho_synth(words,
                                                 synth,
                                                 audio_folder)
            else:
                async_tasks = self.run_word_synth(words,
                                                  synth,
                                                  audio_folder)
            loop.run_until_complete(async_tasks)


class CorporaPhoneticSynthesisTask(BaseCorporaSynthesisTask):
    SYNTH_SUBFOLDER = "phonetic"
    FOR_PHONETIC = True

    def __init__(self, no_confirmation: bool = False,
                 for_corpus: Optional[int] = None,
                 synth_true_words: bool = False):
        super().__init__(no_confirmation, for_corpus)
        self.synth_true_words = synth_true_words

    @staticmethod
    def get_filename(phonemic_form: str):
        return f"{phonemic_form.replace(' ', '_')}.ogg"

    def get_words(self, corpus_path: Path) -> Set[str]:
        phonetic_forms = set()
        for _, word_pho, non_word_pho in CandidatesPairCSV(corpus_path):
            if self.synth_true_words:
                phonetic_forms.update({word_pho, non_word_pho})
            else:
                phonetic_forms.add(non_word_pho)
        return phonetic_forms


class CorporaTextSynthesisTask(BaseCorporaSynthesisTask):
    SYNTH_SUBFOLDER = "text"
    FOR_PHONETIC = False

    @staticmethod
    def get_filename(word: str):
        return f"{word}.ogg"

    def get_words(self, corpus_path: Path) -> Set[str]:
        return {word for word, _, _ in CandidatesPairCSV(corpus_path)}

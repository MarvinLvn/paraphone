import asyncio
import logging
from asyncio import Semaphore
from pathlib import Path
from typing import Optional, Iterable, List, Tuple, Awaitable, Dict

from google.cloud import texttospeech
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

    def estimate_price(self, words_pho: Iterable[str]):
        ssml_template_len = len(self.SSML_TEMPLATE.format(phonemes=""))
        all_words_length = sum(len(w) for w in words_pho)
        return (ssml_template_len + all_words_length) * self.WAVENET_VOICE_PRICE_PER_CHAR

    async def synth_phonemes(self, phonemes: List[str]) -> Tuple[bytes, List[str]]:
        ssml = self.SSML_TEMPLATE.format(phonemes="".join(phonemes))
        synthesis_input = texttospeech.SynthesisInput(ssml=ssml)
        response = await self.client.synthesize_speech(
            input=synthesis_input,
            voice=self.voice,
            audio_config=self.audio_config
        )
        return response.audio_content, phonemes

    async def synth_ssml(self, ssml: str) -> bytes:
        response = await self.client.synthesize_speech(
            input=texttospeech.SynthesisInput(ssml=ssml),
            voice=self.voice,
            audio_config=self.audio_config
        )
        return response.audio_content


class BaseSpeechSynthesisTask(BaseTask, CorporaTaskMixin):
    requires = [
        "synth/credentials.json",
        "corpora/wuggy_pairs/*.csv"
    ]
    MAX_CONCURRENT_CONNEXIONS = 10

    def __init__(self):
        super().__init__()
        self.semaphore = Semaphore(self.MAX_CONCURRENT_CONNEXIONS)

    def store_output(self, audio_bytes: bytes, phonemic_form: str, folder: Path):
        raise NotImplemented()

    async def tasks_limiter(self, task: Awaitable[Tuple[bytes, List[str]]]):
        async with self.semaphore:
            return await task

    async def run_synth(self, words_pho: List[str],
                        synthesizer: GoogleSpeakSynthesizer,
                        output_folder: Path):
        synth_tasks = [self.tasks_limiter(synthesizer.synth_phonemes(word_pho.split(" ")))
                       for word_pho in words_pho]
        for synth_task in async_tqdm.as_completed(synth_tasks):
            audio_bytes, phonemes = await synth_task
            self.store_output(audio_bytes, " ".join(phonemes), output_folder)


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
        synth = GoogleSpeakSynthesizer(lang="fr", voice_id=VOICES["fr"][0],
                                       credentials_path=credentials_path)

        # self.run_synth(list(self.test_words.keys()),
        #                synth,
        #                test_audio_folder)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run_synth(list(self.test_words.keys()),
                                               synth,
                                               test_audio_folder))


class CorporaSynthesisTask(BaseSpeechSynthesisTask):
    creates = [
        "synth/audio/*.mp3"
    ]

    @staticmethod
    def get_filename(phonemic_form: str):
        return f"{phonemic_form.replace(' ', '_')}.ogg"

    def store_output(self, audio_bytes: bytes, phonemic_form: str, folder: Path):
        file_name = phonemic_form.replace(" ", "_")
        with open(folder / Path(self.get_filename(phonemic_form)), "wb") as file:
            file.write(audio_bytes)

    def __init__(self, no_confirmation: bool = False, for_corpus: Optional[int] = None):
        super().__init__()
        self.for_corpus = for_corpus
        self.no_confirmation = no_confirmation

    def run(self, workspace: Workspace):
        credentials_path = workspace.synth / Path("credentials.json")
        corpora = self.find_corpora(workspace.corpora / Path("wuggy_pairs/"))
        if self.for_corpus is not None:
            for corpus_id, corpus_path in corpora:
                if corpus_id == self.for_corpus:
                    corpora = [(corpus_id, corpus_path)]
                    break
            else:
                raise ValueError(f"Couldn't find data for corpus {self.for_corpus}")

        logger.info("Parsing corpus words...")
        all_words = set()
        for corpus_id, corpus_path in tqdm(corpora):
            for _, word_pho, non_word_pho in CandidatesPairCSV(corpus_path):
                all_words.update({word_pho, non_word_pho})
        logger.info(f"Found {len(all_words)} unique words and non-words.")

        lang = workspace.config["lang"]
        voices = VOICES[lang]
        logger.info(f"Using voices {', '.join(voices)} for synthesis.")
        synthesizers = [GoogleSpeakSynthesizer(lang, voice_id, credentials_path)
                        for voice_id in voices]

        synth_words = {
            synth: list(all_words) for synth in synthesizers
        }
        # filtering words that might already have been generated
        for synth, words in list(synth_words.items()):
            audio_folder = workspace.synth / Path(f"audio/{synth.voice_id}/")
            if not audio_folder.exists():
                continue
            elif not list(audio_folder.iterdir()):
                continue
            synth_words[synth] = [
                word for word in words
                if not (audio_folder / Path(self.get_filename(word))).exists()
            ]
            logging.info(f"{len(words) - len(synth_words[synth])} words "
                         f"already exist for {synth.voice_id} and won't be synthesized")

        total_cost = sum(synth.estimate_price(words) for synth, words in synth_words.items())
        logger.info(f"Estimated cost is {total_cost}$")
        if not self.no_confirmation:
            if input("Do you want to proceed?\n[Y/n]").lower() != "y":
                logger.info("Aborting")
                return

        logger.info("Starting synthesis...")
        loop = asyncio.get_event_loop()
        for synth, words in synth_words.items():
            logger.info(f"For synth with voice id {synth.voice_id}")
            audio_folder = workspace.synth / Path(f"audio/{synth.voice_id}/")
            audio_folder.mkdir(parents=True, exist_ok=True)
            loop.run_until_complete(self.run_synth(words,
                                                   synth,
                                                   audio_folder))

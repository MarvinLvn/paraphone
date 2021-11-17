from asyncio import Semaphore
from pathlib import Path
from typing import Optional, Iterable, List, Tuple, Awaitable

from google.cloud import texttospeech
from tqdm.asyncio import tqdm as async_tqdm

from .base import BaseTask, CorporaTaskMixin
from ..workspace import Workspace

tasks_list = []
for f in tqdm.asyncio.tqdm.as_completed(tasks_list):
    await f


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

    async def synth(self, phonemes: List[str]) -> Tuple[bytes, List[str]]:
        ssml = self.SSML_TEMPLATE.format(phonemes="".join(phonemes))
        synthesis_input = texttospeech.SynthesisInput(ssml=ssml)
        response = await self.client.synthesize_speech(
            input=synthesis_input,
            voice=self.voice,
            audio_config=self.audio_config
        )
        return response.audio_content, phonemes


class BaseSpeechSynthesisTask(BaseTask, CorporaTaskMixin):
    requires = [
        "synth/credentials.json",
        "corpora/wuggy_pairs/"
    ]
    MAX_CONCURRENT_CONNEXIONS = 10

    def __init__(self):
        super().__init__()
        self.semaphore = Semaphore(self.MAX_CONCURRENT_CONNEXIONS)

    def store_output(self, audio_bytes: bytes, phonemic_form: str, folder: Path):
        raise NotImplemented()

    async def tasks_limiter(self, tasks: List[Awaitable[Tuple[bytes, List[str]]]]):
        for task in tasks:
            with self.semaphore:
                yield await task

    async def run_synth(self, words_pho: List[str], synthesizer: GoogleSpeakSynthesizer):
        synth_tasks = [synthesizer.synth(word_pho.split(" ")) for word_pho in words_pho]
        request_limited = self.tasks_limiter(synth_tasks)
        async for audio_bytes, phonemes in async_tqdm.as_completed(request_limited,
                                                                   total=len(words_pho)):
            pass  # TODO : store output


class TestSynthesisTask(BaseSpeechSynthesisTask):
    creates = [
        "synth/test/*.mp3"
    ]

    def store_output(self, audio_bytes: bytes, phonemic_form: str, folder: Path):
        pass  # TODO

    def run(self, workspace: Workspace):
        credentials_path = workspace.synth / Path("credential.json")


class CorporaSynthesisTask(BaseSpeechSynthesisTask):
    creates = [
        "synth/audio/*.mp3"
    ]

    def store_output(self, audio_bytes: bytes, phonemic_form: str, folder: Path):
        pass  # TODO

    def __init__(self, no_confirmation: bool = False, for_corpus: Optional[int] = None):
        super().__init__()
        self.for_corpus = for_corpus
        self.no_confirmation = no_confirmation

    def run(self, workspace: Workspace):
        credentials_path = workspace.synth / Path("credential.json")

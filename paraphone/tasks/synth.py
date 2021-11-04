from pathlib import Path
from typing import Optional

from google.cloud import texttospeech
from google.oauth2.service_account import Credentials

from .base import BaseTask
from ..workspace import Workspace


class SynthAudioTask(BaseTask):
    requires = [
        "synth/credentials.json",
        "wuggy/final.csv"
    ]
    creates = [
        "synth/index.csv",
        "synth/audio/*.mp3"
    ]

    SSML_TEMPLATE = """<speak><phoneme alphabet="ipa" ph="{phonemes}"></phoneme></speak>"""

    def __init__(self, no_confirmation: bool):
        super().__init__()
        self.no_confirmation = no_confirmation

    def run(self, workspace: Workspace):
        credentials_path = workspace.synth / Path("credential.json")
        # Instantiates a client
        client = texttospeech.TextToSpeechClient.from_service_account_file(str(credentials_path))

        ssml_text = """<speak><phoneme alphabet="ipa" ph="ˌmænɪˈtoʊbə"></phoneme></speak>
        """

        # Set the text input to be synthesized
        synthesis_input = texttospeech.SynthesisInput(ssml=ssml_text)

        # Build the voice request, select the language code ("en-US") and the ssml
        # voice gender ("neutral")
        voice = texttospeech.VoiceSelectionParams(
            language_code="en-US", ssml_gender=texttospeech.SsmlVoiceGender.NEUTRAL
        )

        # Select the type of audio file you want returned
        audio_config = texttospeech.AudioConfig(
            audio_encoding=texttospeech.AudioEncoding.MP3
        )

        # Perform the text-to-speech request on the text input with the selected
        # voice parameters and audio file type
        response = client.synthesize_speech(
            input=synthesis_input, voice=voice, audio_config=audio_config
        )

        with open("/tmp/output.mp3", "wb") as out:
            # Write the response to the output file.
            out.write(response.audio_content)
#!/usr/bin/env python3
"""
One-shot ChatTTS synthesis for audio quality testing.
Invoked by `make speak-chattts` via docker exec — bypasses Redis entirely.

Usage:
    python speak.py "text to synthesise" [output.mp3]

Reads CHATTTS_* env vars from the running container so it uses the same
speaker embedding and settings as the live worker.
"""
import io
import os
import sys
import torch
import numpy as np
import soundfile as sf
from pathlib import Path
from pydub import AudioSegment

CHATTTS_REF_DIR    = Path(os.environ.get("CHATTTS_REF_DIR",   "/ref"))
CHATTTS_DEVICE     = os.environ.get("CHATTTS_DEVICE",         "cpu")
CHATTTS_SPEED      = int(os.environ.get("CHATTTS_SPEED",      "5"))
CHATTTS_TEMPERATURE = float(os.environ.get("CHATTTS_TEMPERATURE", "0.3"))
CHATTTS_TOP_P      = float(os.environ.get("CHATTTS_TOP_P",    "0.7"))
CHATTTS_TOP_K      = int(os.environ.get("CHATTTS_TOP_K",      "20"))
CHATTTS_SPEAKER_SEED = int(os.environ.get("CHATTTS_SPEAKER_SEED", "42"))

text     = sys.argv[1] if len(sys.argv) > 1 else "Hello, this is a ChatTTS audio quality test."
out_path = sys.argv[2] if len(sys.argv) > 2 else "/tmp/tts-test.mp3"

print(f"Text   : {text[:80]}{'...' if len(text) > 80 else ''}")
print(f"Device : {CHATTTS_DEVICE}  |  speed={CHATTTS_SPEED}  temp={CHATTTS_TEMPERATURE}")

import ChatTTS
chat = ChatTTS.Chat()
chat.load(source="huggingface", device=torch.device(CHATTTS_DEVICE), compile=False)

speaker_path = CHATTTS_REF_DIR / "speaker.pt"
if speaker_path.exists():
    spk_emb = torch.load(str(speaker_path), map_location="cpu", weights_only=True)
    print(f"Speaker: loaded from {speaker_path}")
else:
    torch.manual_seed(CHATTTS_SPEAKER_SEED)
    spk_emb = chat.sample_random_speaker()
    print(f"Speaker: generated fresh (seed={CHATTTS_SPEAKER_SEED})")

params_infer = ChatTTS.Chat.InferCodeParams(
    spk_emb=spk_emb,
    prompt=f"[speed_{CHATTTS_SPEED}]",
    temperature=CHATTTS_TEMPERATURE,
    top_P=CHATTTS_TOP_P,
    top_K=CHATTTS_TOP_K,
)

wavs = chat.infer([text], params_infer_code=params_infer)
wav  = np.array(wavs[0], dtype=np.float32)
dur  = len(wav) / 24000

buf = io.BytesIO()
sf.write(buf, wav, 24000, format="WAV", subtype="PCM_16")
buf.seek(0)
AudioSegment.from_wav(buf).export(out_path, format="mp3", bitrate="128k")

print(f"Output : {out_path}  ({dur:.1f}s, {os.path.getsize(out_path)//1024} KB)")

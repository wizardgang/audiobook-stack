# Reference Audio

Place your reference WAV file here as `reference.wav`.

Requirements:
- Format: WAV (16-bit PCM, mono or stereo)
- Duration: 3–30 seconds of clear speech
- Content: Clean speech with no background noise

Set `F5_REF_TEXT` in your `.env` to the text spoken in the reference audio.
This text is used by F5-TTS to align the reference voice for zero-shot cloning.
If `F5_REF_TEXT` does not match exactly, narration can sound unnaturally fast
or unstable.

Example `.env`:
```
F5_REF_AUDIO=/ref/reference.wav
F5_REF_TEXT=Hello, this is a sample of my voice for the audiobook narrator.
```

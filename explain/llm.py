"""Gemini API call with a JSON schema, so the answer is always machine-readable."""

import json
import os
import time

import requests

API = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
# Tried in order; a model that is unavailable (404) or too slow is skipped. Lite first: fastest, biggest free quota. Override with the GEMINI_MODEL variable.
DEFAULT_MODELS = "gemini-2.5-flash-lite,gemini-2.5-flash,gemini-flash-lite-latest"

CATEGORIES = ["death", "sports", "film_tv_music", "politics_government", "crime_accident",
              "festival_anniversary", "science_tech", "business", "viral_social", "other", "unknown"]

SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "category": {"type": "STRING", "enum": CATEGORIES},
        "reason_en": {"type": "STRING"},
        "reason_native": {"type": "STRING"},
        "confidence": {"type": "STRING", "enum": ["high", "medium", "low"]},
        "evidence_ids": {"type": "ARRAY", "items": {"type": "STRING"}},
    },
    "required": ["category", "reason_en", "reason_native", "confidence", "evidence_ids"],
}


class QuotaExceeded(Exception):
    """The free-tier limit is used up; stop for this run and try again next hour."""


def models() -> list[str]:
    return [m.strip() for m in (os.getenv("GEMINI_MODEL") or DEFAULT_MODELS).split(",") if m.strip()]


def generate(session: requests.Session, api_key: str, prompt: str) -> tuple[dict, dict]:
    """Return (answer, meta). Raises QuotaExceeded on HTTP 429."""
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json", "responseSchema": SCHEMA},
    }
    last_error = None
    for model in models():
        req = json.loads(json.dumps(body))
        if "2.5" in model:  # answer directly; "thinking" makes 2.5 models slow and adds nothing here
            req["generationConfig"]["thinkingConfig"] = {"thinkingBudget": 0 if "flash" in model else 128}
        for attempt in (1, 2):
            started = time.monotonic()
            try:
                resp = session.post(API.format(model=model), json=req, headers={"x-goog-api-key": api_key},
                                    timeout=40)
            except requests.Timeout:
                last_error = f"model {model} timed out"
                break
            if resp.status_code in (400, 404):  # retired model, or a setting this model does not accept
                last_error = f"model {model}: HTTP {resp.status_code} {resp.text[:120]}"
                break
            if resp.status_code == 429:
                raise QuotaExceeded(resp.text[:200])
            if resp.status_code >= 500 and attempt == 1:
                time.sleep(5)
                continue
            resp.raise_for_status()
            data = resp.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"]
            usage = data.get("usageMetadata", {})
            meta = {"model": model, "latency_s": round(time.monotonic() - started, 2),
                    "tokens_in": usage.get("promptTokenCount", 0), "tokens_out": usage.get("candidatesTokenCount", 0)}
            return json.loads(text), meta
    raise RuntimeError(last_error or "no Gemini model answered")

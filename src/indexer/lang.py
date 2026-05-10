from urllib.parse import urlparse

from langdetect import DetectorFactory, detect
from langdetect.lang_detect_exception import LangDetectException

DetectorFactory.seed = 0

PT_DOMAINS = {"oddsagora.com.br"}


def _host(url: str) -> str:
    return urlparse(url).netloc.lower().removeprefix("www.")


def detect_lang(url: str, text: str) -> str:
    host = _host(url)
    if any(host.endswith(d) for d in PT_DOMAINS):
        return "pt"
    sample = (text or "")[:2000].strip()
    if not sample:
        return "en"
    try:
        return "pt" if detect(sample) == "pt" else "en"
    except LangDetectException:
        return "en"

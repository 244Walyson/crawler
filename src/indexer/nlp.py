import re

import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, RSLPStemmer
from nltk.tokenize import word_tokenize

DOMAIN_KEEP = {
    "over",
    "under",
    "handicap",
    "gol",
    "escanteio",
    "odd",
    "odds",
    "draw",
    "empate",
    "corner",
    "set",
    "match",
}


def _ensure_nltk():
    for pkg, path in (
        ("punkt", "tokenizers/punkt"),
        ("punkt_tab", "tokenizers/punkt_tab"),
        ("stopwords", "corpora/stopwords"),
        ("rslp", "stemmers/rslp"),
    ):
        try:
            nltk.data.find(path)
        except LookupError:
            nltk.download(pkg, quiet=True)


_ensure_nltk()

SW_PT = set(stopwords.words("portuguese")) - DOMAIN_KEEP
SW_EN = set(stopwords.words("english")) - DOMAIN_KEEP
RSLP = RSLPStemmer()
PORTER = PorterStemmer()
TOKEN_RE = re.compile(r"^[a-záéíóúâêôãõàç]{2,}$")


def tokenize_and_stem(text: str, lang: str) -> list[str]:
    nltk_lang = "portuguese" if lang == "pt" else "english"
    tokens = [t.lower() for t in word_tokenize(text, language=nltk_lang)]
    sw = SW_PT if lang == "pt" else SW_EN
    stem_fn = RSLP.stem if lang == "pt" else PORTER.stem
    return [stem_fn(t) for t in tokens if TOKEN_RE.match(t) and t not in sw]


def tokenize_and_stem_bilingual(text: str) -> list[str]:
    """Stem with both PT (RSLP) and EN (Porter), filtering stopwords from both languages."""
    all_sw = SW_PT | SW_EN
    tokens = [t.lower() for t in word_tokenize(text, language="english")]
    clean = [t for t in tokens if TOKEN_RE.match(t) and t not in all_sw]
    seen: set[str] = set()
    result: list[str] = []
    for t in clean:
        for stem in (RSLP.stem(t), PORTER.stem(t)):
            if stem not in seen:
                seen.add(stem)
                result.append(stem)
    return result


def content_words(text: str) -> list[str]:
    """Return non-stopword tokens for highlighting (no stemming, both languages)."""
    tokens = [t.lower() for t in word_tokenize(text, language="english")]
    all_sw = SW_PT | SW_EN
    seen: set[str] = set()
    result: list[str] = []
    for t in tokens:
        if TOKEN_RE.match(t) and t not in all_sw and t not in seen:
            seen.add(t)
            result.append(t)
    return result

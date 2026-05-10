import re

from bs4 import BeautifulSoup

NOISE_TAGS = (
    "script",
    "style",
    "nav",
    "footer",
    "header",
    "aside",
    "noscript",
    "form",
    "iframe",
    "svg",
)
_WS_RE = re.compile(r"\s+")


def clean_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(NOISE_TAGS):
        tag.decompose()
    main = soup.find("main") or soup.find("article") or soup.body or soup
    text = main.get_text(separator=" ")
    return _WS_RE.sub(" ", text).strip()

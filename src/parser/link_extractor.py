import re
from typing import List, Tuple
from urllib.parse import urljoin

class LinkExtractor:
    def __init__(self, allowed_domains: List[str]):
        # Store bare domains to allow matching with or without www.
        self.allowed_domains = {d.replace('www.', '') for d in allowed_domains}
        # Match href="url" or href='url'. Using a negated character class is faster than .*?
        self.href_pattern = re.compile(r'href=["\']([^"\']+)["\']', re.IGNORECASE)

        # Fast list of extensions to ignore
        self.ignore_exts = (
            '.png', '.jpg', '.jpeg', '.gif', '.css', '.js', '.ico', '.svg', '.webp',
            '.woff', '.woff2', '.ttf', '.eot', '.mp4', '.mp3', '.pdf', '.zip', '.rar',
            '.xml', '.json', '.csv', '.txt'
        )

    def extract_links(self, html: str, base_url: str) -> List[Tuple[int, str]]:
        links = set()
        for match in self.href_pattern.finditer(html):
            raw_url = match.group(1)
            # Basic cleanup
            if raw_url.startswith(('javascript:', 'mailto:', 'tel:', '#')):
                continue

            full_url = urljoin(base_url, raw_url)

            # Remove fragment
            hash_idx = full_url.find('#')
            if hash_idx != -1:
                full_url = full_url[:hash_idx]

            # Ignore query parameters for extension checking
            parsed_path = full_url.split('?')[0].lower()
            if parsed_path.endswith(self.ignore_exts):
                continue

            # Normalize trailing slash (unless it's just the root domain like https://example.com/)
            if full_url.endswith('/') and full_url.count('/') > 3:
                full_url = full_url[:-1]

            # Quick domain check (assuming full_url is http/https)
            try:
                # e.g., 'https://example.com/path' -> ['https:', '', 'example.com', 'path']
                netloc = full_url.split('/', 3)[2].replace('www.', '')
                if netloc in self.allowed_domains:
                    url_lower = full_url.lower()

                    # Base priority (smaller = higher priority in AdaptiveScheduler)
                    priority = 10

                    # 1. Freshness > Quality: Prioritize live, recent, today
                    if 'live' in url_lower or 'today' in url_lower or 'recent' in url_lower:
                        priority -= 5

                    # 2. Quality > Volume: Ensure we get all sports, de-prioritize soccer/football
                    if 'soccer' in url_lower or 'football' in url_lower:
                        priority += 5
                    elif any(s in url_lower for s in ['basketball', 'tennis', 'hockey', 'volleyball', 'esports', 'handball', 'baseball']):
                        priority -= 3

                    links.add((priority, full_url))
            except IndexError:
                continue

        return list(links)

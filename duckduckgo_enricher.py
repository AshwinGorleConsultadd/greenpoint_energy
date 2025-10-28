"""
duckduckgo_enricher.py
Best-effort enrichment via DuckDuckGo search results (no external libs required).
Uses simple HTML scraping of the search results page to extract basic fields.
"""

from typing import Dict, Any, Optional
import urllib.parse
import urllib.request
import re
import logging
import time


DDG_SEARCH_URL = "https://duckduckgo.com/html/?q={query}"


def _fetch_html(url: str, timeout_seconds: int = 15) -> Optional[str]:
    logger = logging.getLogger("duckduckgo")
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            },
        )
        logger.debug("Fetching URL: %s", url)
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.read().decode(charset, errors="replace")
    except Exception:
        logger.warning("Failed to fetch URL: %s", url)
        return None


def _extract_first_url(html: str) -> Optional[str]:
    # DuckDuckGo HTML returns results with class "result__a" hrefs
    m = re.search(r'<a[^>]+class="result__a"[^>]+href=\"([^\"]+)\"', html)
    if m:
        href = m.group(1)
        # Unwrap DDG redirect links
        if "/l/?kh=" in href and "uddg=" in href:
            try:
                parsed = urllib.parse.urlparse(href)
                qs = urllib.parse.parse_qs(parsed.query)
                return urllib.parse.unquote(qs.get("uddg", [href])[0])
            except Exception:
                return href
        return href
    return None


def _extract_snippets(html: str) -> str:
    # Collect visible snippets as plain text; join to a single blob
    snippets = re.findall(r'<a[^>]+class="result__a"[^>]*>(.*?)</a>', html, flags=re.I | re.S)
    snippets += re.findall(r'<a[^>]+class="result__snippet"[^>]*>(.*?)</a>', html, flags=re.I | re.S)
    text = " ".join(snippets)
    # Strip tags
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _guess_country_from_location(location: Optional[str]) -> Optional[str]:
    if not location:
        return None
    # Simple heuristic based on presence of US state abbreviations or common country names
    if re.search(r",\s*(?:USA|United States|U\.S\.|US)\b", location, re.I):
        return "United States"
    if re.search(r"\bCanada\b", location, re.I):
        return "Canada"
    if re.search(r"\bUK\b|United Kingdom|England|Scotland|Wales", location, re.I):
        return "United Kingdom"
    if re.search(r"\bIndia\b", location, re.I):
        return "India"
    # Fallback: last token after comma may be country
    parts = [p.strip() for p in location.split(",")]
    if len(parts) >= 2:
        candidate = parts[-1]
        if len(candidate) > 2:  # avoid state codes
            return candidate
    return None


def enrich_with_duckduckgo(firm_name: str, location: Optional[str]) -> Dict[str, Any]:
    """Return a partial enrichment dict from DuckDuckGo search results.

    Extracts: website, headquarters (best-effort), country (heuristic), description (snippet),
    linkedin_url (company page if top result contains linkedin), operating_regions (heuristic).
    """
    result: Dict[str, Any] = {
        "website": None,
        "headquarters": None,
        "country": None,
        "description": None,
        "operating_regions": None,
        "linkedin_url": [],
    }

    logger = logging.getLogger("duckduckgo")
    query = urllib.parse.quote_plus(f"{firm_name} company")
    html = _fetch_html(DDG_SEARCH_URL.format(query=query))
    if not html:
        # Fallback: try without "company"
        query = urllib.parse.quote_plus(firm_name)
        html = _fetch_html(DDG_SEARCH_URL.format(query=query))
    if not html:
        # return whatever we have from heuristics
        logger.info("No DDG results. Using heuristics for: %s", firm_name)
        result["country"] = _guess_country_from_location(location)
        return result

    first_url = _extract_first_url(html) or ""
    snippet_blob = _extract_snippets(html)

    # Website: prefer first URL if it looks like company site (not linkedin/wikipedia)
    if first_url and not re.search(r"linkedin|wikipedia|bloomberg|crunchbase|indeed|glassdoor", first_url, re.I):
        result["website"] = first_url
        logger.debug("Website candidate: %s", first_url)

    # LinkedIn link if present prominently
    linkedin_match = re.search(r"https?://[\w\.-]*linkedin\.com/company/[\w\-_/]+", html, re.I)
    if linkedin_match:
        result["linkedin_url"].append({"belongs_to": firm_name, "url": linkedin_match.group(0)})
        logger.debug("LinkedIn URL found in DDG results")

    # Description from snippets (truncate)
    if snippet_blob:
        result["description"] = snippet_blob[:500]

    # Headquarters heuristic from snippet e.g., "Headquartered in City, State" or "based in City, Country"
    hq_match = re.search(r"(?:Headquartered in|Based in)\s+([A-Za-z\s\.-]+,\s*[A-Za-z\s\.-]+)", snippet_blob, re.I)
    if hq_match:
        result["headquarters"] = hq_match.group(1).strip()

    # Operating regions heuristic
    if re.search(r"global|worldwide|international", snippet_blob, re.I):
        result["operating_regions"] = "Global"
    elif re.search(r"North America|USA|United States|Canada|Mexico", snippet_blob, re.I):
        result["operating_regions"] = "North America"

    # Country guess from location if still missing
    result["country"] = result["country"] or _guess_country_from_location(location)

    return result


def enrich_batch_with_duckduckgo(records: list[Dict[str, Any]], batch_size: int = 15, rate_limit_delay: float = 0.4) -> list[Dict[str, Any]]:
    """Enrich all records using DuckDuckGo in internal chunks.

    - records: full list of records
    - batch_size: number of records per internal chunk
    Returns the same list, updated in place with fields:
      website, headquarters, country, description, operating_regions, linkedin_url
    """
    logger = logging.getLogger("duckduckgo")
    logger.info("Starting batch enrichment pipeline (DuckDuckGo -> LLM+Scoring)...")
    total = len(records)
    if total == 0:
        return records
    logger.info("[DDG] Starting enrichment for %d records (chunk size=%d)", total, batch_size)
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        logger.info("[DDG] Processing chunk %d-%d/%d", start + 1, end, total)
        for idx in range(start, end):
            rec = records[idx]
            firm_name = str(rec.get("firm", "")).strip()
            ddg = enrich_with_duckduckgo(firm_name, rec.get("location"))
            rec.update(ddg)
            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)
            logger.debug("[DDG] %d/%d in chunk %d-%d processed: %s", idx - start + 1, end - start, start + 1, end, firm_name or "N/A")
        logger.debug("[DDG] Completed chunk %d-%d/%d", start + 1, end, total)
    logger.info("[DDG] Enrichment completed for %d records", total)
    return records



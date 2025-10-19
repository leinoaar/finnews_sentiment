# finnews_sentiment/etl/enrich_articles.py
import re
import unicodedata
import yaml
from contextlib import closing
from typing import Dict, Iterable, List
from sqlalchemy import select, or_
from ..db import SessionLocal, Article


def load_tickers(cfg_path: str = "configs/tickers.yaml") -> dict:
    """Load ticker configuration (universe, map, aliases) from YAML file."""
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _normalize_text(s: str) -> str:
    """
    Normalize article text:
      - NFC normalize
      - lowercase
      - unify quotes/dashes
      - collapse whitespace
    """
    s = s or ""
    s = unicodedata.normalize("NFC", s)
    s = s.replace("’", "'").replace("‘", "'").replace("”", '"').replace("“", '"')
    s = s.replace("\u2013", "-").replace("\u2014", "-")  # en/em dashes -> hyphen
    s = s.lower()
    # collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _name_to_regex(name: str) -> str:
    """
    Convert a plain company/alias name into a safe regex:
      - keep letters, numbers, spaces and apostrophes
      - collapse spaces to \\s+ to be robust against multiple spaces/newlines
      - allow optional possessive "'s"
    """
    name = (name or "").strip().lower()
    if not name:
        return ""
    # keep only word chars, spaces and apostrophes -> then collapse spaces
    cleaned = re.sub(r"[^\w\s']", " ", name)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""
    # escape each token except apostrophes; then join by \s+
    # split keeps apostrophes inside tokens (e.g., mcdonald's)
    tokens = cleaned.split(" ")
    # escape tokens but keep apostrophes as is inside tokens
    esc_tokens = [re.escape(tok) for tok in tokens]
    pattern_core = r"\s+".join(esc_tokens)
    # optional possessive only if not already endswith "'s"
    if not cleaned.endswith("'s"):
        pattern_core = rf"{pattern_core}('s)?"
    return rf"\b{pattern_core}\b"


def _compile_patterns(universe: Iterable[str],
                      name_map: Dict[str, str],
                      alias_map: Dict[str, List[str]]) -> Dict[str, List[re.Pattern]]:
    """
    Build a list of compiled regex patterns per ticker:
      1) the raw ticker symbol (word-boundaries, case-insensitive)
      2) official mapped company name
      3) aliases
    """
    compiled: Dict[str, List[re.Pattern]] = {}
    for t in universe:
        pats: List[re.Pattern] = []

        # ticker symbol directly, case-insensitive
        pats.append(re.compile(rf"\b{re.escape(t)}\b", re.IGNORECASE))

        # official company name
        official = (name_map.get(t) or "").strip()
        pat_off = _name_to_regex(official)
        if pat_off:
            pats.append(re.compile(pat_off, re.IGNORECASE))

        # aliases
        for alias in (alias_map.get(t) or []):
            pat_alias = _name_to_regex(alias)
            if pat_alias:
                pats.append(re.compile(pat_alias, re.IGNORECASE))

        compiled[t] = pats
    return compiled


def _find_tickers_in_text(text: str, universe: Iterable[str],
                          patterns: Dict[str, List[re.Pattern]]) -> List[str]:
    """Return sorted unique tickers found in the given text."""
    text = _normalize_text(text)
    hits = set()
    for t in universe:
        pats = patterns.get(t, [])
        if any(p.search(text) for p in pats):
            hits.add(t)
    return sorted(hits)


def run(cfg_path: str = "configs/tickers.yaml",
        only_missing: bool = True,
        use_body_text: bool = True,
        batch_commit_every: int = 0,
        limit: int | None = None) -> None:
    """
    Enrich articles with tickers by regex search over title/summary/(optional)text.

    Params
    ------
    cfg_path : str
        Path to YAML config containing `universe`, `map`, `aliases`.
    only_missing : bool
        If True (default), process only articles where `tickers` is NULL/empty.
    use_body_text : bool
        If True, include `Article.text` in matching (in addition to title+summary).
    batch_commit_every : int
        If >0, commit periodically every N updates (useful for very large corpora).
        If 0, commit once at the end.
    limit : Optional[int]
        If set, limit number of articles processed (useful for smoke tests).
    """
    cfg = load_tickers(cfg_path)
    universe = cfg.get("universe", []) or []
    name_map = cfg.get("map", {}) or {}
    alias_map = cfg.get("aliases", {}) or {}

    patterns = _compile_patterns(universe, name_map, alias_map)

    updated = 0
    processed = 0

    with closing(SessionLocal()) as sess:
        q = select(Article)
        if only_missing:
            q = q.where(or_(Article.tickers == None, Article.tickers == ""))  # noqa: E711

        if limit and limit > 0:
            q = q.limit(limit)

        articles = sess.scalars(q).all()

        for art in articles:
            processed += 1
            # Build searchable text
            parts = [art.title or "", art.summary or ""]
            if use_body_text:
                parts.append(art.text or "")
            search_text = " ".join(parts)

            found = _find_tickers_in_text(search_text, universe, patterns)
            new_val = ",".join(found) if found else ""

            # Update only if changed
            if new_val != (art.tickers or ""):
                art.tickers = new_val
                sess.add(art)
                updated += 1

                if batch_commit_every and (updated % batch_commit_every == 0):
                    sess.commit()

        # final commit
        sess.commit()

    print(f" Enriched {updated} / {processed} articles with tickers (regex + aliases)")


if __name__ == "__main__":
    run()

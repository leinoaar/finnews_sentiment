import time
from datetime import datetime
import feedparser
import yaml

from ..db import SessionLocal, Article
from ..settings import settings


def _parse_time(entry) -> datetime:
    """ Try to parse the published time from an RSS entry """
    for k in ("published_parsed", "updated_parsed"):
        dt = getattr(entry, k, None)
        if dt:
            return datetime(*dt[:6])
    return datetime.utcnow()


def run(config_path: str = "configs/sources.yaml", rate_limit_sec: float = 0.3) -> None:
    """Read sources from config file, parse feeds and add to DB. Duplicate URLs are ignored."""

    # First open the db session
    sess = SessionLocal()

    # Read the config file
    with open(config_path, "r", encoding="utf-8") as f:
        sources = yaml.safe_load(f)

    total_inserted = 0

    # Loop over sources
    for src in sources.get("rss", []):
        name = src["name"]
        url = src["url"]

        # Get and parse the feed
        feed = feedparser.parse(url)

        # From each entry, create an Article object
        for e in feed.entries:
            art = Article(
                source=name,
                url=getattr(e, "link", "")[:1024],
                title=getattr(e, "title", "")[:1024],
                published_at=_parse_time(e),
                author=getattr(e, "author", "")[:256],
                summary=getattr(e, "summary", ""),
                text="",      # Fill later with full text
                tickers="",   # Fill later with ticker extraction
            )

            # Try to insert; if URL already exists, rollback
            try:
                sess.add(art)
                sess.commit()
                total_inserted += 1
            except Exception:
                sess.rollback()

        # Rate limiting
        time.sleep(rate_limit_sec)

    print(f"Inserted {total_inserted} new articles from RSS feeds")


if __name__ == "__main__":
    run()

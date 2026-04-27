#!/usr/bin/env python3
"""Build a local SQLite index from arXiv OAI XML pages."""

from __future__ import annotations

import argparse
import html
import re
import sqlite3
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path("/home/sascha/refcheck/CiteFocus")
ARXIV_OAI_DIR = ROOT / "academic_metadata" / "arxiv" / "oai_pages"
DEFAULT_SQLITE_PATH = ARXIV_OAI_DIR / "arxiv_local_index.sqlite"

OAI_NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "raw": "http://arxiv.org/OAI/arXivRaw/",
}
STOP_WORDS = {"a", "an", "the", "of", "and", "or", "for", "to", "in", "on", "with", "by"}
WORD_RE = re.compile(r"[a-zA-Z0-9]+(?:['\\-][a-zA-Z0-9]+)*[?!]?")


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def normalize_title(title: str) -> str:
    title = html.unescape(str(title or ""))
    title = title.lower()
    return re.sub(r"[^a-z0-9]+", "", title)


def normalize_venue(venue: str) -> str:
    venue = html.unescape(str(venue or ""))
    venue = venue.lower()
    venue = re.sub(r"\b(?:vol|volume|no|issue|pp|pages|proc|proceedings)\b", " ", venue)
    return re.sub(r"[^a-z0-9]+", "", venue)


def normalize_doi(doi: str | None) -> str:
    if not doi:
        return ""
    doi = doi.strip()
    doi = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", doi, flags=re.IGNORECASE)
    return doi.rstrip(".,;:").lower()


def get_query_words(title: str, n: int = 8) -> list[str]:
    title = re.sub(r"[{}]", "", str(title or ""))
    all_words = WORD_RE.findall(title)

    def is_significant(word: str) -> bool:
        base = word.rstrip("?!")
        if base.lower() in STOP_WORDS:
            return False
        if len(base) >= 3:
            return True
        has_letter = any(c.isalpha() for c in base)
        has_digit = any(c.isdigit() for c in base)
        return has_letter and has_digit

    significant = [word for word in all_words if is_significant(word)]
    return significant[:n] if len(significant) >= 3 else all_words[:n]


def text_or_empty(node: ET.Element | None) -> str:
    if node is None:
        return ""
    return normalize_space(html.unescape("".join(node.itertext())))


def extract_year_from_raw(raw: ET.Element) -> str:
    version_dates = [text_or_empty(node) for node in raw.findall("./raw:version/raw:date", OAI_NS)]
    for value in version_dates:
        match = re.search(r"\b(19|20)\d{2}\b", value)
        if match:
            return match.group(0)

    journal_ref = text_or_empty(raw.find("./raw:journal-ref", OAI_NS))
    match = re.search(r"\b(19|20)\d{2}\b", journal_ref)
    if match:
        return match.group(0)
    return ""


def extract_record_from_xml_record(record: ET.Element, xml_page: str) -> dict[str, str] | None:
    header = record.find("./oai:header", OAI_NS)
    metadata = record.find("./oai:metadata", OAI_NS)
    raw = metadata.find("./raw:arXivRaw", OAI_NS) if metadata is not None else None
    if header is None or raw is None:
        return None

    arxiv_id = text_or_empty(raw.find("./raw:id", OAI_NS))
    title = text_or_empty(raw.find("./raw:title", OAI_NS))
    if not arxiv_id or not title:
        return None

    oai_identifier = text_or_empty(header.find("./oai:identifier", OAI_NS))
    authors = text_or_empty(raw.find("./raw:authors", OAI_NS))
    abstract = text_or_empty(raw.find("./raw:abstract", OAI_NS))
    journal_ref = text_or_empty(raw.find("./raw:journal-ref", OAI_NS))
    comments = text_or_empty(raw.find("./raw:comments", OAI_NS))
    categories = text_or_empty(raw.find("./raw:categories", OAI_NS))
    doi = normalize_doi(text_or_empty(raw.find("./raw:doi", OAI_NS)))
    year = extract_year_from_raw(raw)
    venue = journal_ref or "arXiv"

    return {
        "arxiv_id": arxiv_id,
        "oai_identifier": oai_identifier,
        "title": title,
        "title_normalized": normalize_title(title),
        "authors": authors,
        "abstract": abstract,
        "venue": venue,
        "venue_normalized": normalize_venue(venue),
        "year": year,
        "doi": doi,
        "comments": comments,
        "categories": categories,
        "url": f"https://arxiv.org/abs/{arxiv_id}",
        "xml_page": xml_page,
    }


def connect_index(sqlite_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            arxiv_id TEXT UNIQUE,
            oai_identifier TEXT,
            title TEXT,
            title_normalized TEXT,
            authors TEXT,
            abstract TEXT,
            venue TEXT,
            venue_normalized TEXT,
            year TEXT,
            doi TEXT,
            comments TEXT,
            categories TEXT,
            url TEXT,
            xml_page TEXT
        );

        CREATE TABLE IF NOT EXISTS title_words (
            word TEXT,
            record_id INTEGER,
            UNIQUE(word, record_id)
        );

        CREATE INDEX IF NOT EXISTS idx_records_arxiv_id ON records(arxiv_id);
        CREATE INDEX IF NOT EXISTS idx_records_doi ON records(doi);
        CREATE INDEX IF NOT EXISTS idx_records_title_normalized ON records(title_normalized);
        CREATE INDEX IF NOT EXISTS idx_records_year ON records(year);
        CREATE INDEX IF NOT EXISTS idx_title_words_word ON title_words(word);
        CREATE INDEX IF NOT EXISTS idx_title_words_word_record ON title_words(word, record_id);
        """
    )
    conn.commit()


def reset_index(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DROP TABLE IF EXISTS title_words;
        DROP TABLE IF EXISTS records;
        """
    )
    conn.commit()


def insert_record_batch(conn: sqlite3.Connection, batch_records: list[dict[str, str]]) -> None:
    rows = [
        (
            record["arxiv_id"],
            record["oai_identifier"],
            record["title"],
            record["title_normalized"],
            record["authors"],
            record["abstract"],
            record["venue"],
            record["venue_normalized"],
            record["year"],
            record["doi"],
            record["comments"],
            record["categories"],
            record["url"],
            record["xml_page"],
        )
        for record in batch_records
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO records (
            arxiv_id, oai_identifier, title, title_normalized, authors, abstract,
            venue, venue_normalized, year, doi, comments, categories, url, xml_page
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    keys = [record["arxiv_id"] for record in batch_records if record["arxiv_id"]]
    if keys:
        placeholders = ",".join("?" for _ in keys)
        id_rows = conn.execute(
            f"SELECT id, title FROM records WHERE arxiv_id IN ({placeholders})",
            keys,
        ).fetchall()
        title_word_rows: list[tuple[str, int]] = []
        for row in id_rows:
            words = {word.lower() for word in get_query_words(row["title"], 8)}
            for word in words:
                title_word_rows.append((word, row["id"]))
        if title_word_rows:
            conn.executemany(
                "INSERT OR IGNORE INTO title_words(word, record_id) VALUES (?, ?)",
                title_word_rows,
            )

    conn.commit()


def build_arxiv_index(conn: sqlite3.Connection, oai_dir: Path, batch_size: int = 10000) -> None:
    create_schema(conn)
    batch_records: list[dict[str, str]] = []
    total_records = 0
    xml_files = sorted(oai_dir.glob("page_*.xml"))

    for idx, xml_file in enumerate(xml_files, start=1):
        root = ET.parse(xml_file).getroot()
        for record in root.findall(".//oai:record", OAI_NS):
            item = extract_record_from_xml_record(record, xml_file.name)
            if item is None:
                continue
            batch_records.append(item)

            if len(batch_records) >= batch_size:
                insert_record_batch(conn, batch_records)
                total_records += len(batch_records)
                print(f"indexed records: {total_records}")
                batch_records.clear()

        if idx % 200 == 0:
            print(f"processed xml pages: {idx}/{len(xml_files)}")

    if batch_records:
        insert_record_batch(conn, batch_records)
        total_records += len(batch_records)

    conn.commit()
    print(f"finished indexing records: {total_records}")


def index_has_data(conn: sqlite3.Connection) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='records'"
    ).fetchone()
    if row is None:
        return False
    count_row = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()
    return bool(count_row and count_row["n"] > 0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a local SQLite index from arXiv OAI XML pages.")
    parser.add_argument("--oai-dir", default=str(ARXIV_OAI_DIR), help="Directory containing arXiv OAI XML pages")
    parser.add_argument("--sqlite-path", default=str(DEFAULT_SQLITE_PATH), help="Output SQLite path")
    parser.add_argument("--batch-size", type=int, default=10000, help="Insert batch size")
    parser.add_argument("--rebuild", action="store_true", help="Drop existing tables and rebuild the index")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    oai_dir = Path(args.oai_dir)
    sqlite_path = Path(args.sqlite_path)

    if not oai_dir.exists():
        print(f"Missing arXiv OAI directory: {oai_dir}", file=sys.stderr)
        return 1

    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect_index(sqlite_path)

    try:
        if args.rebuild and sqlite_path.exists():
            print(f"rebuilding SQLite index at {sqlite_path}")
            reset_index(conn)

        if not index_has_data(conn):
            print(f"building arXiv SQLite index from {oai_dir}")
            build_arxiv_index(conn, oai_dir, batch_size=args.batch_size)
        else:
            print(f"using existing arXiv SQLite index at {sqlite_path}")

        record_count = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"]
        doi_count = conn.execute("SELECT COUNT(*) AS n FROM records WHERE doi != ''").fetchone()["n"]
        abstract_count = conn.execute("SELECT COUNT(*) AS n FROM records WHERE abstract != ''").fetchone()["n"]
        print(f"record_count: {record_count}")
        print(f"doi_count: {doi_count}")
        print(f"abstract_count: {abstract_count}")
        return 0
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower():
            print(
                f"SQLite database is locked. Another process is using {sqlite_path}. "
                "Close the other process and rerun with --rebuild if needed.",
                file=sys.stderr,
            )
            return 1
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Build a local SQLite index from the OpenAlex works snapshot."""

from __future__ import annotations

import argparse
import gzip
import html
import json
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path("/home/sascha/refcheck/CiteFocus")

STOP_WORDS = {"a", "an", "the", "of", "and", "or", "for", "to", "in", "on", "with", "by"}
WORD_RE = re.compile(r"[a-zA-Z0-9]+(?:['\-][a-zA-Z0-9]+)*[?!]?")


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


def decode_abstract_inverted_index(value: object) -> str:
    if not isinstance(value, dict) or not value:
        return ""
    positions_to_words: dict[int, str] = {}
    for word, positions in value.items():
        if not isinstance(word, str) or not isinstance(positions, list):
            continue
        for pos in positions:
            if isinstance(pos, int):
                positions_to_words[pos] = word
    if not positions_to_words:
        return ""
    ordered_words = [positions_to_words[pos] for pos in sorted(positions_to_words)]
    return normalize_space(" ".join(ordered_words))


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


def connect_index(sqlite_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            openalex_id TEXT UNIQUE,
            doi TEXT,
            title TEXT,
            title_normalized TEXT,
            authors TEXT,
            abstract TEXT,
            venue TEXT,
            venue_normalized TEXT,
            publication_year TEXT,
            work_type TEXT,
            source_id TEXT,
            source_type TEXT,
            landing_page_url TEXT,
            indexed_in TEXT,
            updated_date TEXT
        );

        CREATE TABLE IF NOT EXISTS title_words (
            word TEXT,
            record_id INTEGER,
            UNIQUE(word, record_id)
        );

        CREATE INDEX IF NOT EXISTS idx_records_doi ON records(doi);
        CREATE INDEX IF NOT EXISTS idx_records_title_normalized ON records(title_normalized);
        CREATE INDEX IF NOT EXISTS idx_records_publication_year ON records(publication_year);
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


def iter_work_files(works_root: Path):
    for updated_dir in sorted(works_root.iterdir()):
        if not updated_dir.is_dir() or not updated_dir.name.startswith("updated_date="):
            continue
        for gz_path in sorted(updated_dir.glob("part_*.gz")):
            yield updated_dir.name.split("=", 1)[1], gz_path


def extract_work_record(record: dict, updated_date: str) -> dict[str, str] | None:
    title = normalize_space(record.get("title") or record.get("display_name") or "")
    if not title:
        return None

    openalex_id = normalize_space(record.get("id", ""))
    doi = normalize_doi(record.get("doi"))
    publication_year = str(record.get("publication_year") or "")
    work_type = normalize_space(record.get("type", ""))
    abstract = decode_abstract_inverted_index(record.get("abstract_inverted_index"))

    authorships = record.get("authorships") or []
    authors: list[str] = []
    for authorship in authorships:
        if not isinstance(authorship, dict):
            continue
        author = authorship.get("author") or {}
        display_name = ""
        if isinstance(author, dict):
            display_name = normalize_space(author.get("display_name", ""))
        if not display_name:
            display_name = normalize_space(authorship.get("raw_author_name", ""))
        if display_name:
            authors.append(display_name)

    primary_location = record.get("primary_location") or {}
    best_oa_location = record.get("best_oa_location") or {}
    location = primary_location or best_oa_location
    source = location.get("source") or {}
    venue = normalize_space(source.get("display_name", "")) or normalize_space(
        location.get("raw_source_name", "")
    )
    source_id = normalize_space(source.get("id", ""))
    source_type = normalize_space(source.get("type", ""))
    landing_page_url = normalize_space(location.get("landing_page_url", ""))
    indexed_in = "; ".join(str(item) for item in (record.get("indexed_in") or []) if item)

    return {
        "openalex_id": openalex_id,
        "doi": doi,
        "title": title,
        "title_normalized": normalize_title(title),
        "authors": "; ".join(authors),
        "abstract": abstract,
        "venue": venue,
        "venue_normalized": normalize_venue(venue),
        "publication_year": publication_year,
        "work_type": work_type,
        "source_id": source_id,
        "source_type": source_type,
        "landing_page_url": landing_page_url,
        "indexed_in": indexed_in,
        "updated_date": updated_date,
    }


def insert_record_batch(conn: sqlite3.Connection, batch_records: list[dict[str, str]]) -> None:
    rows = [
        (
            record["openalex_id"],
            record["doi"],
            record["title"],
            record["title_normalized"],
            record["authors"],
            record["abstract"],
            record["venue"],
            record["venue_normalized"],
            record["publication_year"],
            record["work_type"],
            record["source_id"],
            record["source_type"],
            record["landing_page_url"],
            record["indexed_in"],
            record["updated_date"],
        )
        for record in batch_records
    ]

    conn.executemany(
        """
        INSERT OR REPLACE INTO records (
            openalex_id, doi, title, title_normalized, authors, abstract,
            venue, venue_normalized, publication_year, work_type,
            source_id, source_type, landing_page_url, indexed_in, updated_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    keys = [record["openalex_id"] for record in batch_records if record["openalex_id"]]
    if keys:
        placeholders = ",".join("?" for _ in keys)
        id_rows = conn.execute(
            f"SELECT id, title FROM records WHERE openalex_id IN ({placeholders})",
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


def build_openalex_index(conn: sqlite3.Connection, works_root: Path, batch_size: int = 10000) -> None:
    create_schema(conn)
    batch_records: list[dict[str, str]] = []
    total_records = 0

    for updated_date, gz_path in iter_work_files(works_root):
        with gzip.open(gz_path, "rt", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    raw_record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                record = extract_work_record(raw_record, updated_date)
                if record is None:
                    continue
                batch_records.append(record)

                if len(batch_records) >= batch_size:
                    insert_record_batch(conn, batch_records)
                    total_records += len(batch_records)
                    print(f"indexed records: {total_records}")
                    batch_records.clear()

    if batch_records:
        insert_record_batch(conn, batch_records)
        total_records += len(batch_records)

    conn.commit()
    print(f"finished indexing records: {total_records}")


def parse_args() -> argparse.Namespace:
    openalex_dir = ROOT / "academic_metadata" / "openalex"
    parser = argparse.ArgumentParser(description="Build a local SQLite index from OpenAlex works.")
    parser.add_argument("--works-root", default=str(openalex_dir / "data" / "works"))
    parser.add_argument("--sqlite-path", default=str(openalex_dir / "openalex_local_index.sqlite"))
    parser.add_argument("--rebuild", action="store_true", help="Drop and rebuild the SQLite index.")
    parser.add_argument("--batch-size", type=int, default=10000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    works_root = Path(args.works_root)
    sqlite_path = Path(args.sqlite_path)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    if not works_root.exists():
        print(f"Missing OpenAlex works directory: {works_root}", file=sys.stderr)
        return 1

    try:
        conn = connect_index(sqlite_path)
        if args.rebuild and sqlite_path.exists():
            print(f"rebuilding SQLite index at {sqlite_path}")
            reset_index(conn)

        build_openalex_index(conn, works_root, batch_size=args.batch_size)

        record_count = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"]
        doi_count = conn.execute("SELECT COUNT(*) AS n FROM records WHERE doi != ''").fetchone()["n"]
        venue_count = conn.execute("SELECT COUNT(*) AS n FROM records WHERE venue != ''").fetchone()["n"]
        abstract_count = conn.execute("SELECT COUNT(*) AS n FROM records WHERE abstract != ''").fetchone()["n"]
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower():
            print(
                "SQLite database is locked. Another process is still using "
                f"{sqlite_path}. Close the notebook/kernel or other Python process "
                "that has the DB open, then rerun with --rebuild.",
                file=sys.stderr,
            )
            return 1
        raise

    print("record_count:", record_count)
    print("doi_count:", doi_count)
    print("venue_count:", venue_count)
    print("abstract_count:", abstract_count)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Build a local SQLite index from the DBLP XML dump."""

from __future__ import annotations

import argparse
import gzip
import html
import html.entities
import re
import sqlite3
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path("/home/sascha/refcheck/CiteFocus")

PUBLICATION_TAGS = {
    "article",
    "inproceedings",
    "proceedings",
    "book",
    "incollection",
    "phdthesis",
    "mastersthesis",
    "www",
}

STOP_WORDS = {"a", "an", "the", "of", "and", "or", "for", "to", "in", "on", "with", "by"}
WORD_RE = re.compile(r"[a-zA-Z0-9]+(?:['\-][a-zA-Z0-9]+)*[?!]?")
DOI_RE = re.compile(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.IGNORECASE)


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def normalize_title(title: str) -> str:
    title = html.unescape(str(title or ""))
    title = title.lower()
    title = re.sub(r"[^a-z0-9]+", "", title)
    return title


def normalize_venue(venue: str) -> str:
    venue = html.unescape(str(venue or ""))
    venue = venue.lower()
    venue = re.sub(r"\b(?:vol|volume|no|issue|pp|pages|proc|proceedings)\b", " ", venue)
    venue = re.sub(r"[^a-z0-9]+", "", venue)
    return venue


def normalize_doi(doi: str | None) -> str:
    if not doi:
        return ""
    doi = doi.strip()
    doi = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", doi, flags=re.IGNORECASE)
    return doi.rstrip(".,;:").lower()


def extract_doi_from_text(text: str) -> str:
    if not text:
        return ""
    text = normalize_space(text)
    text = re.sub(r"^https?://(?:dx\.)?doi\.org/", "", text, flags=re.IGNORECASE)
    match = DOI_RE.search(text)
    return normalize_doi(match.group(1)) if match else ""


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


def make_dblp_xml_parser() -> ET.XMLParser:
    parser = ET.XMLParser()
    entity_map = {
        "amp": "&",
        "lt": "<",
        "gt": ">",
        "quot": '"',
        "apos": "'",
    }
    for name, value in html.entities.html5.items():
        clean_name = name[:-1] if name.endswith(";") else name
        entity_map.setdefault(clean_name, value)
    parser.entity.update(entity_map)
    return parser


def open_dblp_source(xml_path: Path, xml_gz_path: Path):
    if xml_path.exists():
        return xml_path.open("rb")
    if xml_gz_path.exists():
        return gzip.open(xml_gz_path, "rb")
    raise FileNotFoundError(f"Could not find DBLP source at {xml_path} or {xml_gz_path}")


def get_child_text(elem: ET.Element, child_name: str) -> str:
    child = elem.find(child_name)
    if child is None:
        return ""
    return normalize_space("".join(child.itertext()))


def extract_record_from_elem(elem: ET.Element) -> dict[str, str] | None:
    record_type = elem.tag
    if record_type not in PUBLICATION_TAGS:
        return None

    dblp_key = elem.attrib.get("key", "")
    title = get_child_text(elem, "title")
    if not title:
        return None

    authors = [normalize_space("".join(child.itertext())) for child in elem.findall("author")]
    authors = [author for author in authors if author]
    if not authors:
        editors = [normalize_space("".join(child.itertext())) for child in elem.findall("editor")]
        authors = [author for author in editors if author]

    journal = get_child_text(elem, "journal")
    booktitle = get_child_text(elem, "booktitle")
    school = get_child_text(elem, "school")
    publisher = get_child_text(elem, "publisher")
    venue = journal or booktitle or school or publisher
    year = get_child_text(elem, "year")

    ee_values = [normalize_space("".join(child.itertext())) for child in elem.findall("ee")]
    ee_values = [value for value in ee_values if value]
    ee = ee_values[0] if ee_values else ""

    doi = ""
    for value in ee_values:
        doi = extract_doi_from_text(value)
        if doi:
            break

    return {
        "dblp_key": dblp_key,
        "record_type": record_type,
        "title": title,
        "title_normalized": normalize_title(title),
        "authors": "; ".join(authors),
        "venue": venue,
        "venue_normalized": normalize_venue(venue),
        "year": year,
        "doi": doi,
        "ee": ee,
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
            dblp_key TEXT UNIQUE,
            record_type TEXT,
            title TEXT,
            title_normalized TEXT,
            authors TEXT,
            venue TEXT,
            venue_normalized TEXT,
            year TEXT,
            doi TEXT,
            ee TEXT
        );

        CREATE TABLE IF NOT EXISTS title_words (
            word TEXT,
            record_id INTEGER,
            UNIQUE(word, record_id)
        );

        CREATE INDEX IF NOT EXISTS idx_records_doi ON records(doi);
        CREATE INDEX IF NOT EXISTS idx_records_title_normalized ON records(title_normalized);
        CREATE INDEX IF NOT EXISTS idx_records_year ON records(year);
        CREATE INDEX IF NOT EXISTS idx_title_words_word ON title_words(word);
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
            record["dblp_key"],
            record["record_type"],
            record["title"],
            record["title_normalized"],
            record["authors"],
            record["venue"],
            record["venue_normalized"],
            record["year"],
            record["doi"],
            record["ee"],
        )
        for record in batch_records
    ]

    conn.executemany(
        """
        INSERT OR IGNORE INTO records (
            dblp_key, record_type, title, title_normalized, authors,
            venue, venue_normalized, year, doi, ee
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    keys = [record["dblp_key"] for record in batch_records if record["dblp_key"]]
    if keys:
        placeholders = ",".join("?" for _ in keys)
        id_rows = conn.execute(
            f"SELECT id, title FROM records WHERE dblp_key IN ({placeholders})",
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


def build_dblp_index(
    conn: sqlite3.Connection,
    xml_path: Path,
    xml_gz_path: Path,
    batch_size: int = 5000,
) -> None:
    create_schema(conn)
    parser = make_dblp_xml_parser()
    batch_records: list[dict[str, str]] = []
    total_records = 0

    with open_dblp_source(xml_path, xml_gz_path) as handle:
        context = ET.iterparse(handle, events=("start", "end"), parser=parser)
        _, root = next(context)

        for event, elem in context:
            if event != "end" or elem.tag not in PUBLICATION_TAGS:
                continue

            record = extract_record_from_elem(elem)
            if record is not None:
                batch_records.append(record)

            if len(batch_records) >= batch_size:
                insert_record_batch(conn, batch_records)
                total_records += len(batch_records)
                print(f"indexed records: {total_records}")
                batch_records.clear()

            elem.clear()
            root.clear()

    if batch_records:
        insert_record_batch(conn, batch_records)
        total_records += len(batch_records)

    conn.commit()
    print(f"finished indexing records: {total_records}")


def parse_args() -> argparse.Namespace:
    dblp_dir = ROOT / "academic_metadata" / "dblp"
    parser = argparse.ArgumentParser(description="Build a local SQLite index from DBLP XML.")
    parser.add_argument("--xml-path", default=str(dblp_dir / "dblp.xml"))
    parser.add_argument("--xml-gz-path", default=str(dblp_dir / "dblp.xml.gz"))
    parser.add_argument("--sqlite-path", default=str(dblp_dir / "dblp_local_index.sqlite"))
    parser.add_argument("--rebuild", action="store_true", help="Drop and rebuild the SQLite index.")
    parser.add_argument("--batch-size", type=int, default=5000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    xml_path = Path(args.xml_path)
    xml_gz_path = Path(args.xml_gz_path)
    sqlite_path = Path(args.sqlite_path)
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        conn = connect_index(sqlite_path)
        if args.rebuild and sqlite_path.exists():
            print(f"rebuilding SQLite index at {sqlite_path}")
            reset_index(conn)

        build_dblp_index(conn, xml_path, xml_gz_path, batch_size=args.batch_size)

        record_count = conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"]
        doi_count = conn.execute("SELECT COUNT(*) AS n FROM records WHERE doi != ''").fetchone()["n"]
        venue_count = conn.execute("SELECT COUNT(*) AS n FROM records WHERE venue != ''").fetchone()["n"]
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

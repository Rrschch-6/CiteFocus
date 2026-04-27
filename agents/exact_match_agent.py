"""Exact match agent for CiteFocus."""

from __future__ import annotations

import argparse
import html
import json
import re
import sqlite3
from pathlib import Path
from typing import Any


DEFAULT_PARSED_PATH = "/home/sascha/refcheck/CiteFocus/outputs/parsed_citations.json"
DEFAULT_ROUTE_PATH = "/home/sascha/refcheck/CiteFocus/outputs/route_plan.json"
DEFAULT_OUTPUT_PATH = "/home/sascha/refcheck/CiteFocus/outputs/exact_matches.json"

ARXIV_DB_PATH = Path("/home/sascha/refcheck/CiteFocus/academic_metadata/arxiv/oai_pages/arxiv_local_index.sqlite")
DBLP_DB_PATH = Path("/home/sascha/refcheck/CiteFocus/academic_metadata/dblp/dblp_local_index.sqlite")
OPENALEX_DB_PATH = Path("/home/sascha/refcheck/CiteFocus/academic_metadata/openalex/openalex_local_index.sqlite")

ARXIV_NEW_RE = re.compile(r"\b(\d{4}\.\d{4,5}(?:v\d+)?)\b")
ARXIV_OLD_RE = re.compile(r"\b([a-z\-]+/\d{7}(?:v\d+)?)\b", re.IGNORECASE)
SURNAME_PREFIXES = {"van", "von", "de", "del", "della", "di", "da", "al", "el", "la", "le", "ben", "ibn", "mac", "mc", "o"}
NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def load_json(path: str) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON list in {path}")
    return data


def save_json(path: str, data: list[dict[str, Any]]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    print(f"[exact_match_agent] Wrote JSON to: {output_path}")


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def normalize_title(text: str | None) -> str:
    if not text:
        return ""
    text = html.unescape(str(text))
    text = text.lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def normalize_doi(doi: str | None) -> str:
    if not doi:
        return ""
    doi = doi.strip()
    doi = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", doi, flags=re.IGNORECASE)
    return doi.rstrip(".,;:").lower()


def extract_arxiv_id(text: str | None) -> str | None:
    text = str(text or "").strip()
    if not text:
        return None
    match = re.search(r"arXiv[:\s]+(\d{4}\.\d{4,5}(?:v\d+)?)", text, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"arxiv\.org/abs/(\d{4}\.\d{4,5}(?:v\d+)?)", text, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"arXiv[:\s]+([a-z\-]+/\d{7}(?:v\d+)?)", text, re.IGNORECASE)
    if match:
        return match.group(1)
    match = ARXIV_NEW_RE.search(text)
    if match:
        return match.group(1)
    match = ARXIV_OLD_RE.search(text)
    if match:
        return match.group(1)
    return None


def get_surname_from_parts(parts: list[str]) -> str:
    if not parts:
        return ""
    while len(parts) >= 2 and parts[-1].lower().rstrip(".") in NAME_SUFFIXES:
        parts = parts[:-1]
    if len(parts) >= 3 and parts[-3].lower().rstrip(".") in SURNAME_PREFIXES:
        return " ".join(parts[-3:])
    if len(parts) >= 2 and parts[-2].lower().rstrip(".") in SURNAME_PREFIXES:
        return " ".join(parts[-2:])
    return parts[-1]


def normalize_author(name: str) -> str:
    name = normalize_space(name)
    if not name:
        return ""
    if "," in name:
        parts = name.split(",", 1)
        surname = parts[0].strip().lower()
        initials = parts[1].strip() if len(parts) > 1 else ""
        first_initial = initials[0].lower() if initials else ""
        return f"{first_initial} {surname}".strip()
    parts = [part for part in name.split() if part]
    if not parts:
        return ""
    surname = get_surname_from_parts(parts).lower()
    first_initial = parts[0][0].lower()
    return f"{first_initial} {surname}".strip()


def author_overlap_score(ref_authors: list[str], cand_authors: list[str]) -> float:
    ref_set = {normalize_author(author) for author in ref_authors if normalize_author(author)}
    cand_set = {normalize_author(author) for author in cand_authors if normalize_author(author)}
    if not ref_set or not cand_set:
        return 0.0
    overlap = len(ref_set & cand_set)
    return overlap / max(1, len(ref_set))


def split_authors(text: str | None) -> list[str]:
    if not text:
        return []
    return [normalize_space(part) for part in str(text).split(";") if normalize_space(part)]


def parse_authors_loose(text: str | None) -> list[str]:
    normalized = normalize_space(text or "")
    if not normalized:
        return []
    if ";" in normalized:
        return split_authors(normalized)
    if " and " in normalized.lower():
        normalized = re.sub(r"\s+(and|&)\s+", ";", normalized, flags=re.IGNORECASE)
        return [normalize_space(part) for part in normalized.split(";") if normalize_space(part)]
    return [normalize_space(part) for part in normalized.split(",") if normalize_space(part)]


def sqlite_connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def build_route_map(route_plan: list[dict[str, Any]]) -> dict[Any, dict[str, Any]]:
    return {record.get("citation_id"): record for record in route_plan}


def make_not_run_result(citation_id: Any, route_record: dict[str, Any]) -> dict[str, Any]:
    return {
        "citation_id": citation_id,
        "db_priority": route_record.get("db_priority", []),
        "run_exact_match": route_record.get("run_exact_match", False),
        "match_found": False,
        "match_type": "skipped",
        "matched_db": None,
        "matched_record": None,
        "confidence": route_record.get("confidence", 0.0),
    }


def choose_best_exact_candidate(citation: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not candidates:
        return None
    parsed_authors = citation.get("parsed_authors") or []
    parsed_year = citation.get("parsed_year")
    best_candidate = None
    best_key = None
    for candidate in candidates:
        author_score = author_overlap_score(parsed_authors, candidate.get("authors", []))
        cand_year = candidate.get("year")
        year_exact = int(parsed_year is not None and cand_year is not None and int(parsed_year) == int(cand_year))
        year_neutral = int(parsed_year is None or cand_year is None)
        key = (year_exact, author_score, year_neutral)
        if best_key is None or key > best_key:
            best_key = key
            best_candidate = candidate
    return best_candidate


def row_to_arxiv_candidate(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "db": "arxiv",
        "record_id": row["arxiv_id"],
        "title": row["title"],
        "title_normalized": row["title_normalized"],
        "authors": parse_authors_loose(row["authors"]),
        "abstract": row["abstract"] or None,
        "year": int(row["year"]) if str(row["year"]).isdigit() else None,
        "venue": row["venue"],
        "doi": normalize_doi(row["doi"]) or None,
        "url": row["url"],
        "oai_identifier": row["oai_identifier"],
        "comments": row["comments"],
        "categories": row["categories"],
    }


def row_to_dblp_candidate(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "db": "dblp",
        "record_id": row["dblp_key"],
        "title": row["title"],
        "title_normalized": row["title_normalized"],
        "authors": split_authors(row["authors"]),
        "abstract": row["abstract"] if "abstract" in row.keys() and row["abstract"] else None,
        "year": int(row["year"]) if str(row["year"]).isdigit() else None,
        "venue": row["venue"],
        "doi": normalize_doi(row["doi"]) or None,
        "url": row["ee"],
    }


def row_to_openalex_candidate(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "db": "openalex",
        "record_id": row["openalex_id"],
        "title": row["title"],
        "title_normalized": row["title_normalized"],
        "authors": split_authors(row["authors"]),
        "abstract": row["abstract"] or None,
        "year": int(row["publication_year"]) if str(row["publication_year"]).isdigit() else None,
        "venue": row["venue"],
        "doi": normalize_doi(row["doi"]) or None,
        "url": row["landing_page_url"] or row["openalex_id"],
    }


def exact_match_sqlite(citation: dict[str, Any], connection: sqlite3.Connection, *, db_name: str) -> tuple[str, dict[str, Any]] | None:
    parsed_doi = normalize_doi(citation.get("parsed_doi"))
    title_normalized = normalize_title(citation.get("parsed_title"))

    if db_name == "arxiv":
        candidate_factory = row_to_arxiv_candidate
        title_col = "title_normalized"
        year_col = "year"
        parsed_arxiv_id = extract_arxiv_id(citation.get("parsed_arxiv_id")) or extract_arxiv_id(citation.get("raw_citation")) or extract_arxiv_id(citation.get("parsed_url"))
        if parsed_arxiv_id:
            row = connection.execute("SELECT * FROM records WHERE arxiv_id = ? LIMIT 1", (parsed_arxiv_id,)).fetchone()
            if row is not None:
                return "arxiv_id_exact", candidate_factory(row)
    elif db_name == "dblp":
        candidate_factory = row_to_dblp_candidate
        title_col = "title_normalized"
        year_col = "year"
    else:
        candidate_factory = row_to_openalex_candidate
        title_col = "title_normalized"
        year_col = "publication_year"

    if parsed_doi:
        rows = connection.execute("SELECT * FROM records WHERE lower(doi) = ? LIMIT 20", (parsed_doi,)).fetchall()
        candidates = [candidate_factory(row) for row in rows]
        candidate = choose_best_exact_candidate(citation, candidates)
        if candidate is not None:
            return "doi_exact", candidate

    if title_normalized:
        rows = connection.execute(f"SELECT * FROM records WHERE {title_col} = ? LIMIT 50", (title_normalized,)).fetchall()
        candidates = [candidate_factory(row) for row in rows]
        candidate = choose_best_exact_candidate(citation, candidates)
        if candidate is not None:
            parsed_year = citation.get("parsed_year")
            if parsed_year is not None and candidate.get("year") is not None and int(parsed_year) == int(candidate["year"]):
                return "title_year_exact", candidate
            return "title_exact", candidate

    return None


def run_exact_match_for_citation(citation: dict[str, Any], route_record: dict[str, Any], *, arxiv_conn: sqlite3.Connection, dblp_conn: sqlite3.Connection, openalex_conn: sqlite3.Connection) -> dict[str, Any]:
    citation_id = citation.get("citation_id")
    db_priority = route_record.get("db_priority", [])
    if not route_record.get("run_exact_match", False):
        return make_not_run_result(citation_id, route_record)

    for db_name in db_priority:
        if db_name == "arxiv":
            result = exact_match_sqlite(citation, arxiv_conn, db_name="arxiv")
        elif db_name == "dblp":
            result = exact_match_sqlite(citation, dblp_conn, db_name="dblp")
        elif db_name == "openalex":
            result = exact_match_sqlite(citation, openalex_conn, db_name="openalex")
        else:
            result = None
        if result is None:
            continue
        match_type, candidate = result
        confidence = 0.99 if match_type in {"arxiv_id_exact", "doi_exact"} else 0.93 if match_type == "title_year_exact" else 0.88
        return {
            "citation_id": citation_id,
            "db_priority": db_priority,
            "run_exact_match": True,
            "match_found": True,
            "match_type": match_type,
            "matched_db": candidate["db"],
            "matched_record": candidate,
            "confidence": confidence,
        }

    return {
        "citation_id": citation_id,
        "db_priority": db_priority,
        "run_exact_match": True,
        "match_found": False,
        "match_type": "not_found",
        "matched_db": None,
        "matched_record": None,
        "confidence": 0.0,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run exact matching for CiteFocus citations.")
    parser.add_argument("--parsed", default=DEFAULT_PARSED_PATH, help="Path to parsed_citations.json")
    parser.add_argument("--route", default=DEFAULT_ROUTE_PATH, help="Path to route_plan.json")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Path to exact_matches.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    parsed_records = load_json(args.parsed)
    route_plan = load_json(args.route)
    route_map = build_route_map(route_plan)
    arxiv_conn = sqlite_connect(ARXIV_DB_PATH)
    dblp_conn = sqlite_connect(DBLP_DB_PATH)
    openalex_conn = sqlite_connect(OPENALEX_DB_PATH)
    try:
        results = []
        for citation in parsed_records:
            citation_id = citation.get("citation_id")
            route_record = route_map.get(citation_id, {"citation_id": citation_id, "db_priority": ["openalex", "dblp", "arxiv"], "run_exact_match": False, "run_lexical_retrieval": True, "run_dense_retrieval": True, "confidence": 0.0})
            print(f"[exact_match_agent] citation_id={citation_id} run_exact={route_record.get('run_exact_match')}")
            results.append(run_exact_match_for_citation(citation, route_record, arxiv_conn=arxiv_conn, dblp_conn=dblp_conn, openalex_conn=openalex_conn))
    finally:
        arxiv_conn.close()
        dblp_conn.close()
        openalex_conn.close()

    save_json(args.output, results)
    print(f"[exact_match_agent] Done. Processed {len(results)} citations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Lexical retrieval agent for CiteFocus."""

from __future__ import annotations

import argparse
import difflib
import html
import json
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


DEFAULT_PARSED_PATH = "/home/sascha/refcheck/CiteFocus/outputs/parsed_citations.json"
DEFAULT_ROUTE_PATH = "/home/sascha/refcheck/CiteFocus/outputs/route_plan.json"
DEFAULT_EXACT_PATH = "/home/sascha/refcheck/CiteFocus/outputs/exact_matches.json"
DEFAULT_OUTPUT_PATH = "/home/sascha/refcheck/CiteFocus/outputs/lexical_candidates.json"
DB_MODE_CHOICES = ("all", "arxiv", "dblp", "openalex")

ARXIV_DB_PATH = Path("/home/sascha/refcheck/CiteFocus/academic_metadata/arxiv/oai_pages/arxiv_local_index.sqlite")
DBLP_DB_PATH = Path("/home/sascha/refcheck/CiteFocus/academic_metadata/dblp/dblp_local_index.sqlite")
OPENALEX_DB_PATH = Path("/home/sascha/refcheck/CiteFocus/academic_metadata/openalex/openalex_local_index.sqlite")

STOP_WORDS = {"a", "an", "the", "of", "and", "or", "for", "to", "in", "on", "with", "by"}
WORD_RE = re.compile(r"[a-zA-Z0-9]+(?:['\-][a-zA-Z0-9]+)*[?!]?")
SURNAME_PREFIXES = {"van", "von", "de", "del", "della", "di", "da", "al", "el", "la", "le", "ben", "ibn", "mac", "mc", "o"}
NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}
SQL_FETCH_MULTIPLIER = 2
LEXICAL_CANDIDATES_PER_DB = 2
LEXICAL_TOTAL_CANDIDATES = 5
MIN_ACCEPTABLE_LEXICAL_SCORE = 0.45
MIN_TITLE_OVERLAP_SCORE = 0.60
MIN_MATCHED_WORDS = 2
NEAR_PERFECT_TITLE_SCORE = 0.85
NEAR_PERFECT_AUTHOR_SCORE = 0.50
NEAR_PERFECT_LEXICAL_SCORE = 0.75
WEAK_TITLE_SCORE = 0.75
WEAK_LEXICAL_SCORE = 0.65
BACKUP_SCORE_GAP = 0.05
CROSSREF_ROWS = 5
CROSSREF_TIMEOUT_SECONDS = 10.0
CROSSREF_USER_AGENT = "Academic Reference Parser"


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
    print(f"[lexical_retrieval_agent] Wrote JSON to: {output_path}")


def build_record_map(records: list[dict[str, Any]]) -> dict[Any, dict[str, Any]]:
    return {record.get("citation_id"): record for record in records}


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


def get_query_words(text: str, n: int = 8) -> list[str]:
    text = re.sub(r"[{}]", "", str(text or ""))
    all_words = WORD_RE.findall(text)

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
    return [word.lower() for word in (significant[:n] if len(significant) >= 3 else all_words[:n])]


def build_query_words(citation: dict[str, Any]) -> list[str]:
    title = normalize_space(citation.get("parsed_title"))
    if title:
        return get_query_words(title, 8)
    raw = normalize_space(citation.get("raw_citation"))
    if raw:
        return get_query_words(raw, 8)
    return []


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


def title_overlap_score(query_words: list[str], candidate_title: str) -> float:
    query_set = {word.lower() for word in query_words if word}
    candidate_set = set(get_query_words(candidate_title, 12))
    if not query_set or not candidate_set:
        return 0.0
    overlap = len(query_set & candidate_set)
    return overlap / max(1, len(query_set))


def minimum_required_matches(query_words: list[str]) -> int:
    if not query_words:
        return 1
    if len(query_words) <= 2:
        return 1
    return min(MIN_MATCHED_WORDS, len(query_words))


def crossref_titles_match(title_a: str, title_b: str) -> bool:
    norm_a = normalize_title(title_a)
    norm_b = normalize_title(title_b)
    if not norm_a or not norm_b:
        return False

    if difflib.SequenceMatcher(None, norm_a, norm_b).ratio() >= 0.95:
        return True

    shorter, longer = (norm_a, norm_b) if len(norm_a) <= len(norm_b) else (norm_b, norm_a)
    if len(shorter) < 30 or not longer.startswith(shorter):
        return False

    def has_subtitle(text: str) -> bool:
        lower = str(text or "").lower()
        subtitle_pos = max(lower.rfind("?"), lower.rfind("!"))
        if subtitle_pos == -1:
            return False
        return any(char.isalnum() for char in lower[subtitle_pos + 1 :])

    if has_subtitle(title_a) != has_subtitle(title_b):
        coverage = len(shorter) / max(1, len(longer))
        return coverage >= 0.70

    return True


def year_support_score(parsed_year: Any, candidate_year: Any) -> float:
    if parsed_year is None or candidate_year is None:
        return 0.5
    try:
        parsed_year = int(parsed_year)
        candidate_year = int(candidate_year)
    except Exception:
        return 0.5
    if parsed_year == candidate_year:
        return 1.0
    if abs(parsed_year - candidate_year) <= 1:
        return 0.7
    if abs(parsed_year - candidate_year) <= 2:
        return 0.3
    return 0.0


def score_candidate(citation: dict[str, Any], candidate: dict[str, Any], *, matched_word_count: int | None = None, force_title_match: bool = False) -> dict[str, Any] | None:
    query_words = build_query_words(citation)
    if not query_words:
        return None

    parsed_authors = citation.get("parsed_authors") or []
    parsed_title_normalized = normalize_title(citation.get("parsed_title"))
    parsed_has_identifier = bool(normalize_doi(citation.get("parsed_doi")) or citation.get("parsed_arxiv_id"))
    parsed_year = citation.get("parsed_year")
    parsed_year_int: int | None = None
    if parsed_year is not None:
        try:
            parsed_year_int = int(parsed_year)
        except Exception:
            parsed_year_int = None

    candidate = dict(candidate)
    title_score = title_overlap_score(query_words, str(candidate.get("title") or ""))
    if force_title_match:
        title_score = max(title_score, 0.95)

    author_score = author_overlap_score(parsed_authors, candidate.get("authors") or [])
    year_score = year_support_score(parsed_year_int, candidate.get("year"))
    candidate_title_normalized = normalize_title(candidate.get("title"))
    normalized_title_exact = bool(
        parsed_title_normalized
        and candidate_title_normalized
        and parsed_title_normalized == candidate_title_normalized
    )
    if normalized_title_exact:
        title_score = 1.0

    lexical_score = 0.70 * title_score + 0.20 * author_score + 0.10 * year_score
    if normalized_title_exact:
        lexical_score += 0.10

    if title_score < MIN_TITLE_OVERLAP_SCORE:
        return None
    if title_score < WEAK_TITLE_SCORE and year_score >= 0.7:
        return None
    if author_score == 0.0 and not parsed_has_identifier and title_score < 0.9:
        return None

    if matched_word_count is None:
        matched_word_count = len(set(query_words) & set(get_query_words(str(candidate.get("title") or ""), 12)))

    candidate.update(
        {
            "matched_word_count": int(matched_word_count),
            "title_score": round(title_score, 4),
            "author_score": round(author_score, 4),
            "year_score": round(year_score, 4),
            "lexical_score": round(min(1.0, lexical_score), 4),
            "normalized_title_exact": normalized_title_exact,
        }
    )
    return candidate


def sqlite_connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def row_to_arxiv_candidate(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "db": "arxiv",
        "record_id": row["arxiv_id"],
        "title": row["title"],
        "authors": parse_authors_loose(row["authors"]),
        "year": int(row["year"]) if str(row["year"]).isdigit() else None,
        "venue": row["venue"],
        "doi": normalize_doi(row["doi"]) or None,
        "url": row["url"],
        "abstract": row["abstract"] or None,
    }


def row_to_dblp_candidate(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "db": "dblp",
        "record_id": row["dblp_key"],
        "title": row["title"],
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
        "authors": split_authors(row["authors"]),
        "year": int(row["publication_year"]) if str(row["publication_year"]).isdigit() else None,
        "venue": row["venue"],
        "doi": normalize_doi(row["doi"]) or None,
        "url": row["landing_page_url"] or row["openalex_id"],
        "abstract": row["abstract"] or None,
    }


def first_text(value: Any) -> str:
    if isinstance(value, list):
        for item in value:
            if isinstance(item, str) and normalize_space(item):
                return normalize_space(item)
        return ""
    return normalize_space(value)


def clean_crossref_abstract(value: Any) -> str | None:
    text = normalize_space(value)
    if not text:
        return None
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = normalize_space(text)
    return text or None


def extract_crossref_year(item: dict[str, Any]) -> int | None:
    for field_name in ("published-print", "published-online", "issued", "created"):
        field = item.get(field_name) or {}
        date_parts = field.get("date-parts") or []
        if not date_parts or not isinstance(date_parts[0], list) or not date_parts[0]:
            continue
        try:
            return int(date_parts[0][0])
        except Exception:
            continue
    return None


def query_crossref_candidates(citation: dict[str, Any], limit: int = CROSSREF_ROWS) -> list[dict[str, Any]]:
    title = normalize_space(citation.get("parsed_title")) or normalize_space(citation.get("raw_citation"))
    if not title:
        return []

    query_words = get_query_words(title, 6)
    if not query_words:
        return []

    query = " ".join(query_words)
    url = f"https://api.crossref.org/works?query.title={urllib.parse.quote(query)}&rows={limit}"
    request = urllib.request.Request(url, headers={"User-Agent": CROSSREF_USER_AGENT})

    try:
        with urllib.request.urlopen(request, timeout=CROSSREF_TIMEOUT_SECONDS) as response:
            payload = json.load(response)
    except urllib.error.URLError as exc:
        print(f"[lexical_retrieval_agent] crossref_query_failed reason={exc}")
        return []
    except Exception as exc:
        print(f"[lexical_retrieval_agent] crossref_query_failed reason={exc}")
        return []

    items = payload.get("message", {}).get("items", [])
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        found_title = first_text(item.get("title"))
        if not found_title or not crossref_titles_match(title, found_title):
            continue

        authors = [
            normalize_space(f"{author.get('given', '')} {author.get('family', '')}")
            for author in item.get("author", [])
            if normalize_space(f"{author.get('given', '')} {author.get('family', '')}")
        ]
        if not authors:
            continue

        doi = normalize_doi(item.get("DOI")) or None
        record_id = doi or item.get("URL") or normalize_title(found_title)
        key = ("crossref", str(record_id))
        if key in seen:
            continue
        seen.add(key)

        candidate = {
            "db": "crossref",
            "record_id": record_id,
            "title": found_title,
            "authors": authors,
            "year": extract_crossref_year(item),
            "venue": first_text(item.get("container-title")) or None,
            "doi": doi,
            "url": f"https://doi.org/{doi}" if doi else item.get("URL"),
            "abstract": clean_crossref_abstract(item.get("abstract")),
        }
        scored_candidate = score_candidate(citation, candidate, force_title_match=True)
        if scored_candidate is not None:
            candidates.append(scored_candidate)

    candidates.sort(
        key=lambda item: (
            bool(item.get("normalized_title_exact")),
            item["lexical_score"],
            item["matched_word_count"],
            bool(item.get("abstract")),
        ),
        reverse=True,
    )
    return candidates[:limit]


def lexical_search_sqlite(citation: dict[str, Any], connection: sqlite3.Connection, *, db_name: str, limit: int = 5) -> list[dict[str, Any]]:
    query_words = build_query_words(citation)
    if not query_words:
        return []

    if db_name == "arxiv":
        row_to_candidate = row_to_arxiv_candidate
        year_col = "year"
    elif db_name == "dblp":
        row_to_candidate = row_to_dblp_candidate
        year_col = "year"
    else:
        row_to_candidate = row_to_openalex_candidate
        year_col = "publication_year"

    placeholders = ",".join("?" for _ in query_words)
    min_word_matches = minimum_required_matches(query_words)
    parsed_year = citation.get("parsed_year")
    parsed_year_int: int | None = None
    year_min: int | None = None
    year_max: int | None = None
    if parsed_year is not None:
        try:
            parsed_year_int = int(parsed_year)
            year_min = parsed_year_int - 1
            year_max = parsed_year_int + 1
        except Exception:
            parsed_year = None
            parsed_year_int = None

    sql = f"""
        SELECT r.*, tw.matched_word_count
        FROM (
            SELECT record_id, COUNT(*) AS matched_word_count
            FROM title_words
            WHERE word IN ({placeholders})
            GROUP BY record_id
            HAVING COUNT(*) >= ?
            ORDER BY matched_word_count DESC
            LIMIT ?
        ) AS tw
        JOIN records r ON r.id = tw.record_id
        WHERE (
            ? IS NULL OR
            r.{year_col} = '' OR
            r.{year_col} IS NULL OR
            CAST(r.{year_col} AS INTEGER) BETWEEN ? AND ?
        )
        ORDER BY tw.matched_word_count DESC
    """
    params: list[Any] = [
        *query_words,
        min_word_matches,
        max(limit * SQL_FETCH_MULTIPLIER, limit),
        parsed_year_int,
        year_min,
        year_max,
    ]
    rows = connection.execute(sql, params).fetchall()

    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        candidate = row_to_candidate(row)
        key = (candidate["db"], str(candidate["record_id"]))
        if key in seen:
            continue
        seen.add(key)
        scored_candidate = score_candidate(citation, candidate, matched_word_count=int(row["matched_word_count"]))
        if scored_candidate is not None:
            candidates.append(scored_candidate)

    candidates.sort(key=lambda item: (bool(item.get("normalized_title_exact")), item["lexical_score"], item["matched_word_count"], bool(item.get("abstract"))), reverse=True)
    return candidates[:limit]


def should_skip_lexical(route_record: dict[str, Any], exact_record: dict[str, Any] | None) -> tuple[bool, str | None]:
    if not route_record.get("run_lexical_retrieval", False):
        return True, "router_disabled"
    if exact_record is None:
        return False, None
    if exact_record.get("match_found"):
        return True, f"exact_match_found:{exact_record.get('match_type')}"
    return False, None


def merge_candidates(candidates_by_db: list[tuple[str, list[dict[str, Any]]]], total_limit: int = 10) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for db_rank, (_, candidates) in enumerate(candidates_by_db, start=1):
        for candidate in candidates:
            key = (candidate["db"], str(candidate["record_id"]))
            if key in seen:
                continue
            seen.add(key)
            candidate["db_priority_rank"] = db_rank
            merged.append(candidate)
    merged.sort(key=lambda item: (bool(item.get("normalized_title_exact")), item["lexical_score"], -item["db_priority_rank"], item["matched_word_count"], bool(item.get("abstract"))), reverse=True)
    final_candidates = merged[:total_limit]
    for idx, candidate in enumerate(final_candidates, start=1):
        candidate["retrieval_rank"] = idx
    return final_candidates


def build_skip_result(citation_id: Any, route_record: dict[str, Any], *, reason: str) -> dict[str, Any]:
    return {
        "citation_id": citation_id,
        "db_priority": route_record.get("db_priority", []),
        "effective_db_priority": [],
        "run_lexical_retrieval": route_record.get("run_lexical_retrieval", False),
        "skipped": True,
        "skip_reason": reason,
        "query_words": [],
        "db_timings_ms": {},
        "candidates": [],
    }


def reorder_db_priority_for_lexical(db_priority: list[str]) -> list[str]:
    allowed = {"arxiv", "dblp", "openalex"}
    ordered = [db_name for db_name in db_priority if db_name in allowed]
    seen: set[str] = set()
    deduped: list[str] = []
    for db_name in ordered:
        if db_name in seen:
            continue
        seen.add(db_name)
        deduped.append(db_name)
    return deduped


def resolve_lexical_db_priority(db_priority: list[str], *, db_mode: str) -> list[str]:
    if db_mode == "all":
        return reorder_db_priority_for_lexical(db_priority)
    if db_mode in {"arxiv", "dblp", "openalex"}:
        return [db_mode]
    raise ValueError(f"Unsupported lexical db_mode: {db_mode}")


def is_near_perfect_candidate(candidate: dict[str, Any] | None) -> bool:
    if not candidate:
        return False
    return (
        float(candidate.get("title_score") or 0.0) >= NEAR_PERFECT_TITLE_SCORE
        and float(candidate.get("author_score") or 0.0) >= NEAR_PERFECT_AUTHOR_SCORE
        and float(candidate.get("lexical_score") or 0.0) >= NEAR_PERFECT_LEXICAL_SCORE
    )


def is_weak_candidate(candidate: dict[str, Any] | None) -> bool:
    if not candidate:
        return True
    return (
        float(candidate.get("title_score") or 0.0) < WEAK_TITLE_SCORE
        or float(candidate.get("lexical_score") or 0.0) < WEAK_LEXICAL_SCORE
    )


def query_db_candidates(citation: dict[str, Any], db_name: str, *, arxiv_conn: sqlite3.Connection, dblp_conn: sqlite3.Connection, openalex_conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if db_name == "arxiv":
        return lexical_search_sqlite(citation, arxiv_conn, db_name="arxiv", limit=LEXICAL_CANDIDATES_PER_DB)
    if db_name == "dblp":
        return lexical_search_sqlite(citation, dblp_conn, db_name="dblp", limit=LEXICAL_CANDIDATES_PER_DB)
    if db_name == "openalex":
        return lexical_search_sqlite(citation, openalex_conn, db_name="openalex", limit=LEXICAL_CANDIDATES_PER_DB)
    return []


def retrieve_lexical_for_citation(citation: dict[str, Any], route_record: dict[str, Any], exact_record: dict[str, Any] | None, *, arxiv_conn: sqlite3.Connection, dblp_conn: sqlite3.Connection, openalex_conn: sqlite3.Connection, db_mode: str = "all") -> dict[str, Any]:
    citation_id = citation.get("citation_id")
    skip, reason = should_skip_lexical(route_record, exact_record)
    if skip:
        return build_skip_result(citation_id, route_record, reason=reason or "skipped")

    query_words = build_query_words(citation)
    db_priority = route_record.get("db_priority", [])
    effective_db_priority = resolve_lexical_db_priority(db_priority, db_mode=db_mode)
    if not effective_db_priority:
        return {
            "citation_id": citation_id,
            "db_priority": db_priority,
            "effective_db_priority": [],
            "lexical_db_mode": db_mode,
            "run_lexical_retrieval": True,
            "skipped": False,
            "skip_reason": "no_eligible_databases",
            "query_words": query_words,
            "db_timings_ms": {},
            "candidates": [],
        }
    candidates_by_db: list[tuple[str, list[dict[str, Any]]]] = []
    db_timings_ms: dict[str, float] = {}
    attempted_dbs: list[str] = []

    def run_db(db_name: str) -> None:
        start_time = time.perf_counter()
        db_candidates = query_db_candidates(citation, db_name, arxiv_conn=arxiv_conn, dblp_conn=dblp_conn, openalex_conn=openalex_conn)
        db_timings_ms[db_name] = round((time.perf_counter() - start_time) * 1000, 2)
        candidates_by_db.append((db_name, db_candidates))
        attempted_dbs.append(db_name)

    for db_name in effective_db_priority:
        run_db(db_name)

    candidates = merge_candidates(candidates_by_db, total_limit=LEXICAL_TOTAL_CANDIDATES)
    if not candidates and db_mode == "all":
        crossref_start_time = time.perf_counter()
        crossref_candidates = query_crossref_candidates(citation, limit=LEXICAL_CANDIDATES_PER_DB)
        db_timings_ms["crossref"] = round((time.perf_counter() - crossref_start_time) * 1000, 2)
        attempted_dbs.append("crossref")
        if crossref_candidates:
            candidates = merge_candidates([("crossref", crossref_candidates)], total_limit=LEXICAL_TOTAL_CANDIDATES)

    return {
        "citation_id": citation_id,
        "db_priority": db_priority,
        "effective_db_priority": attempted_dbs,
        "lexical_db_mode": db_mode,
        "run_lexical_retrieval": True,
        "skipped": False,
        "skip_reason": None,
        "query_words": query_words,
        "db_timings_ms": db_timings_ms,
        "candidates": candidates,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run lexical retrieval for CiteFocus citations.")
    parser.add_argument("--parsed", default=DEFAULT_PARSED_PATH, help="Path to parsed_citations.json")
    parser.add_argument("--route", default=DEFAULT_ROUTE_PATH, help="Path to route_plan.json")
    parser.add_argument("--exact", default=DEFAULT_EXACT_PATH, help="Path to exact_matches.json")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Path to lexical_candidates.json")
    parser.add_argument("--db-mode", choices=DB_MODE_CHOICES, default="all", help="Choose a single DB or 'all' to follow router DB order.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    parsed_records = load_json(args.parsed)
    route_map = build_record_map(load_json(args.route))
    exact_map = build_record_map(load_json(args.exact))
    arxiv_conn = sqlite_connect(ARXIV_DB_PATH)
    dblp_conn = sqlite_connect(DBLP_DB_PATH)
    openalex_conn = sqlite_connect(OPENALEX_DB_PATH)
    try:
        results = []
        for citation in parsed_records:
            citation_id = citation.get("citation_id")
            route_record = route_map.get(citation_id, {"citation_id": citation_id, "db_priority": ["openalex", "dblp", "arxiv"], "run_exact_match": False, "run_lexical_retrieval": True, "run_dense_retrieval": True, "confidence": 0.0})
            exact_record = exact_map.get(citation_id)
            result = retrieve_lexical_for_citation(citation, route_record, exact_record, arxiv_conn=arxiv_conn, dblp_conn=dblp_conn, openalex_conn=openalex_conn, db_mode=args.db_mode)
            status = "skipped" if result.get("skipped") else "retrieved"
            skip_reason = result.get("skip_reason")
            candidate_count = len(result.get("candidates", []))
            timing_summary = ", ".join(f"{db_name}={elapsed}ms" for db_name, elapsed in result.get("db_timings_ms", {}).items())
            if skip_reason:
                print(f"[lexical_retrieval_agent] citation_id={citation_id} status={status} reason={skip_reason} candidates={candidate_count}" + (f" timings=[{timing_summary}]" if timing_summary else ""))
            else:
                print(f"[lexical_retrieval_agent] citation_id={citation_id} status={status} candidates={candidate_count}" + (f" timings=[{timing_summary}]" if timing_summary else ""))
            results.append(result)
    finally:
        arxiv_conn.close()
        dblp_conn.close()
        openalex_conn.close()

    save_json(args.output, results)
    print(f"[lexical_retrieval_agent] Done. Processed {len(results)} citations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

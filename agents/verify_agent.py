"""Deterministic bibliographic verifier for CiteFocus."""

from __future__ import annotations

import argparse
import html
import json
import re
from pathlib import Path
from typing import Any


DEFAULT_PARSED_PATH = "/home/sascha/refcheck/CiteFocus/outputs/parsed_citations.json"
DEFAULT_FUSED_PATH = "/home/sascha/refcheck/CiteFocus/outputs/fused_candidates.json"
DEFAULT_OUTPUT_PATH = "/home/sascha/refcheck/CiteFocus/outputs/verification_results.json"

ARXIV_NEW_RE = re.compile(r"\b(\d{4}\.\d{4,5}(?:v\d+)?)\b")
ARXIV_OLD_RE = re.compile(r"\b([a-z\-]+/\d{7}(?:v\d+)?)\b", re.IGNORECASE)
SURNAME_PREFIXES = {"van", "von", "de", "del", "della", "di", "da", "al", "el", "la", "le", "ben", "ibn", "mac", "mc", "o"}
NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}
HIGH_CONFIDENCE_EXACT_TYPES = {"arxiv_id_exact", "doi_exact"}


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
    print(f"[verify_agent] Wrote JSON to: {output_path}")


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


def normalize_venue(text: str | None) -> str:
    if not text:
        return ""
    text = html.unescape(str(text)).lower()
    text = re.sub(r"\b(?:vol|volume|no|issue|pp|pages|proc|proceedings)\b", " ", text)
    return re.sub(r"[^a-z0-9]+", "", text)


def normalize_doi(doi: str | None) -> str:
    if not doi:
        return ""
    doi = doi.strip()
    doi = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", doi, flags=re.IGNORECASE)
    return doi.rstrip(".,;:").lower()


def normalize_url(url: str | None) -> str:
    if not url:
        return ""
    return normalize_space(url).rstrip("/").lower()


def get_query_words(text: str, n: int = 12) -> list[str]:
    words = re.findall(r"[a-zA-Z0-9]+(?:['\-][a-zA-Z0-9]+)*", str(text or ""))
    return [word.lower() for word in words[:n]]


def word_overlap_ratio(a: str | None, b: str | None) -> float:
    set_a = set(get_query_words(a))
    set_b = set(get_query_words(b))
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / max(1, len(set_a))


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
    return len(ref_set & cand_set) / max(1, len(ref_set))


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


def evaluate_title_match(parsed_title: str | None, candidate_title: str | None) -> tuple[str, float]:
    norm_parsed = normalize_title(parsed_title)
    norm_candidate = normalize_title(candidate_title)
    if not norm_parsed or not norm_candidate:
        return "unknown", 0.5
    if norm_parsed == norm_candidate:
        return "exact", 1.0
    overlap = word_overlap_ratio(parsed_title, candidate_title)
    if overlap >= 0.75:
        return "partial", 0.75
    return "mismatch", 0.0


def evaluate_author_match(parsed_authors: list[str], candidate_authors: list[str]) -> tuple[str, float]:
    if not parsed_authors or not candidate_authors:
        return "unknown", 0.5
    overlap = author_overlap_score(parsed_authors, candidate_authors)
    if overlap >= 0.8:
        return "exact", 1.0
    if overlap > 0.0:
        return "partial", 0.65
    return "mismatch", 0.0


def evaluate_year_match(parsed_year: Any, candidate_year: Any) -> tuple[str, float]:
    if parsed_year is None or candidate_year is None:
        return "unknown", 0.5
    try:
        parsed_year = int(parsed_year)
        candidate_year = int(candidate_year)
    except Exception:
        return "unknown", 0.5
    if parsed_year == candidate_year:
        return "exact", 1.0
    if abs(parsed_year - candidate_year) <= 1:
        return "partial", 0.65
    return "mismatch", 0.0


def evaluate_venue_match(parsed_venue: str | None, candidate_venue: str | None) -> tuple[str, float]:
    norm_parsed = normalize_venue(parsed_venue)
    norm_candidate = normalize_venue(candidate_venue)
    if not norm_parsed or not norm_candidate:
        return "unknown", 0.5
    if norm_parsed == norm_candidate:
        return "exact", 1.0
    if norm_parsed in norm_candidate or norm_candidate in norm_parsed:
        return "partial", 0.7
    overlap = word_overlap_ratio(parsed_venue, candidate_venue)
    if overlap >= 0.5:
        return "partial", 0.6
    return "mismatch", 0.0


def evaluate_doi_match(parsed_doi: str | None, candidate_doi: str | None) -> tuple[str, float]:
    norm_parsed = normalize_doi(parsed_doi)
    norm_candidate = normalize_doi(candidate_doi)
    if not norm_parsed or not norm_candidate:
        return "unknown", 0.5
    if norm_parsed == norm_candidate:
        return "exact", 1.0
    return "mismatch", 0.0


def evaluate_url_match(parsed_url: str | None, candidate_url: str | None) -> tuple[str, float]:
    norm_parsed = normalize_url(parsed_url)
    norm_candidate = normalize_url(candidate_url)
    if not norm_parsed or not norm_candidate:
        return "unknown", 0.5
    if norm_parsed == norm_candidate:
        return "exact", 1.0
    if norm_parsed in norm_candidate or norm_candidate in norm_parsed:
        return "partial", 0.7
    return "mismatch", 0.0


def evaluate_arxiv_match(parsed_record: dict[str, Any], candidate: dict[str, Any]) -> tuple[str, float]:
    parsed_arxiv = extract_arxiv_id(parsed_record.get("parsed_arxiv_id")) or extract_arxiv_id(parsed_record.get("raw_citation")) or extract_arxiv_id(parsed_record.get("parsed_url"))
    candidate_arxiv = extract_arxiv_id(candidate.get("record_id")) or extract_arxiv_id(candidate.get("url")) or extract_arxiv_id(candidate.get("oai_identifier"))
    if not parsed_arxiv or not candidate_arxiv:
        return "unknown", 0.5
    if parsed_arxiv == candidate_arxiv:
        return "exact", 1.0
    return "mismatch", 0.0


def compute_bibliographic_score(title_score: float, author_score: float, year_score: float, doi_score: float, url_score: float, arxiv_score: float, fused_record: dict[str, Any]) -> float:
    score = 0.40 * title_score + 0.25 * author_score + 0.15 * year_score + 0.10 * doi_score + 0.05 * url_score + 0.05 * arxiv_score
    if fused_record.get("selected_match_type") in HIGH_CONFIDENCE_EXACT_TYPES and title_score > 0.0:
        score = max(score, 0.92)
    return min(1.0, round(score, 4))


def determine_overall_status(fused_record: dict[str, Any], title_match: str, author_match: str, year_match: str, doi_match: str, arxiv_match: str, bibliographic_score: float) -> str:
    if fused_record.get("selected_candidate") is None:
        return "hallucinated"
    if fused_record.get("selected_match_type") in HIGH_CONFIDENCE_EXACT_TYPES and title_match != "mismatch":
        return "verified"
    if title_match == "mismatch":
        return "hallucinated"
    if title_match == "partial" or author_match in {"partial", "mismatch"} or year_match in {"partial", "mismatch"}:
        return "partially_verified"
    if title_match == "exact":
        return "verified"
    return "partially_verified"


def determine_verification_category_and_subcategory(
    parsed_record: dict[str, Any],
    fused_record: dict[str, Any],
    *,
    title_match: str,
    author_match: str,
    year_match: str,
    doi_match: str,
    arxiv_match: str,
    bibliographic_score: float,
) -> tuple[str, str, list[str]]:
    candidate = fused_record.get("selected_candidate")

    if candidate is None:
        return "Not Verified", "no_candidate_found", ["no_candidate_found"]

    if title_match == "mismatch":
        return "Not Verified", "title_mismatch", ["title_mismatch"]

    if fused_record.get("selected_match_type") in HIGH_CONFIDENCE_EXACT_TYPES and title_match != "mismatch":
        return "Verified", "exact_identifier_match", ["exact_identifier_match"]

    if (
        title_match == "exact"
        and author_match not in {"partial", "mismatch"}
        and year_match not in {"partial", "mismatch"}
        and bibliographic_score >= 0.75
    ):
        return "Verified", "strong_metadata_match", ["strong_metadata_match"]

    reasons: list[str] = []
    if title_match == "partial":
        reasons.append("minor_title_mismatch")
    if author_match in {"partial", "mismatch"}:
        reasons.append("author_mismatch")
    if year_match in {"partial", "mismatch"}:
        reasons.append("year_mismatch")

    if any(reason in {"author_mismatch", "year_mismatch"} for reason in reasons):
        return "Partially Verified", "author/year mismatch", reasons
    if "minor_title_mismatch" in reasons:
        return "Partially Verified", "minor_title_mismatch", reasons
    return "Partially Verified", "author/year mismatch", ["author_mismatch", "year_mismatch"]


def build_explanation(fused_record: dict[str, Any], title_match: str, author_match: str, year_match: str, doi_match: str, arxiv_match: str, overall_status: str) -> str:
    if fused_record.get("selected_candidate") is None:
        return "No fused candidate was available for bibliographic verification."
    parts = [f"Selected candidate came from {fused_record.get('selected_source') or 'unknown'}", f"title={title_match}", f"authors={author_match}", f"year={year_match}"]
    if doi_match != "unknown":
        parts.append(f"doi={doi_match}")
    if arxiv_match != "unknown":
        parts.append(f"arxiv_id={arxiv_match}")
    parts.append(f"overall_status={overall_status}")
    return "; ".join(parts) + "."


def verify_one(parsed_record: dict[str, Any], fused_record: dict[str, Any]) -> dict[str, Any]:
    citation_id = parsed_record.get("citation_id")
    candidate = fused_record.get("selected_candidate")
    if not candidate:
        return {"citation_id": citation_id, "selected_source": None, "selected_match_type": None, "selected_candidate_title": None, "selected_candidate_db": None, "field_verification": {"title_match": "unknown", "author_match": "unknown", "year_match": "unknown", "doi_match": "unknown", "url_match": "unknown", "arxiv_id_match": "unknown", "bibliographic_score": 0.0}, "overall_status": "hallucinated", "verification_category": "Not Verified", "verification_subcategory": "no_candidate_found", "verification_reasons": ["no_candidate_found"], "overall_confidence": 0.0, "explanation": "No fused candidate was available for verification."}

    title_match, title_score = evaluate_title_match(parsed_record.get("parsed_title"), candidate.get("title"))
    author_match, author_score = evaluate_author_match(parsed_record.get("parsed_authors") or [], candidate.get("authors") or [])
    year_match, year_score = evaluate_year_match(parsed_record.get("parsed_year"), candidate.get("year"))
    doi_match, doi_score = evaluate_doi_match(parsed_record.get("parsed_doi"), candidate.get("doi"))
    url_match, url_score = evaluate_url_match(parsed_record.get("parsed_url"), candidate.get("url"))
    arxiv_match, arxiv_score = evaluate_arxiv_match(parsed_record, candidate)
    bibliographic_score = compute_bibliographic_score(title_score, author_score, year_score, doi_score, url_score, arxiv_score, fused_record)
    overall_status = determine_overall_status(fused_record, title_match, author_match, year_match, doi_match, arxiv_match, bibliographic_score)
    verification_category, verification_subcategory, verification_reasons = determine_verification_category_and_subcategory(
        parsed_record,
        fused_record,
        title_match=title_match,
        author_match=author_match,
        year_match=year_match,
        doi_match=doi_match,
        arxiv_match=arxiv_match,
        bibliographic_score=bibliographic_score,
    )
    overall_confidence = max(float(fused_record.get("selected_confidence") or 0.0), bibliographic_score)
    explanation = build_explanation(fused_record, title_match, author_match, year_match, doi_match, arxiv_match, overall_status)

    return {
        "citation_id": citation_id,
        "selected_source": fused_record.get("selected_source"),
        "selected_match_type": fused_record.get("selected_match_type"),
        "selected_candidate_title": candidate.get("title"),
        "selected_candidate_db": candidate.get("db"),
        "field_verification": {
            "title_match": title_match,
            "author_match": author_match,
            "year_match": year_match,
            "doi_match": doi_match,
            "url_match": url_match,
            "arxiv_id_match": arxiv_match,
            "bibliographic_score": bibliographic_score,
        },
        "overall_status": overall_status,
        "verification_category": verification_category,
        "verification_subcategory": verification_subcategory,
        "verification_reasons": verification_reasons,
        "overall_confidence": round(overall_confidence, 4),
        "explanation": explanation,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify fused CiteFocus candidates bibliographically.")
    parser.add_argument("--parsed", default=DEFAULT_PARSED_PATH, help="Path to parsed_citations.json")
    parser.add_argument("--fused", default=DEFAULT_FUSED_PATH, help="Path to fused_candidates.json")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Path to verification_results.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    parsed_records = load_json(args.parsed)
    fused_map = build_record_map(load_json(args.fused))
    results = []
    for parsed_record in parsed_records:
        citation_id = parsed_record.get("citation_id")
        fused_record = fused_map.get(citation_id, {"citation_id": citation_id, "selected_source": None, "selected_match_type": None, "selected_candidate": None, "selected_confidence": 0.0})
        result = verify_one(parsed_record, fused_record)
        print(f"[verify_agent] citation_id={citation_id} status={result['overall_status']} confidence={result['overall_confidence']}")
        results.append(result)
    save_json(args.output, results)
    print(f"[verify_agent] Done. Processed {len(results)} citations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

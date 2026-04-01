"""CiteFocus parse agent.

Reads a PDF, extracts bibliography entries with hallucinator-derived parsing
logic, finds simple numeric citation contexts from the body text, and writes a
JSON list.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils import parse_references_from_text

try:
    import fitz
except ImportError as exc:  # pragma: no cover
    raise SystemExit("PyMuPDF is required. Install it with: pip install PyMuPDF") from exc


DEFAULT_INPUT_PATH = "/home/sascha/refcheck/CiteFocus/inputs/nest.pdf"
DEFAULT_OUTPUT_PATH = "/home/sascha/refcheck/CiteFocus/outputs/parsed_citations.json"
REFERENCE_HEADING_PATTERN = re.compile(
    r"^\s*(references|bibliography|works cited)\s*$",
    re.IGNORECASE | re.MULTILINE,
)
BODY_CITATION_PATTERN = re.compile(r"\[([0-9,\-–—\s]+)\]")


def extract_text_by_page(pdf_path: str) -> tuple[list[str], str]:
    print(f"[parse_agent] Reading PDF: {pdf_path}")
    document = fitz.open(pdf_path)
    try:
        pages = [page.get_text() for page in document]
    finally:
        document.close()
    return pages, "\n".join(pages)


def split_document_sections(full_text: str) -> tuple[str, str]:
    match = REFERENCE_HEADING_PATTERN.search(full_text)
    if not match:
        return full_text.strip(), ""
    return full_text[:match.start()].strip(), full_text[match.start():].strip()


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def split_into_sentences(body_text: str) -> list[str]:
    normalized = normalize_space(body_text)
    if not normalized:
        return []
    sentences = re.split(r"(?<=[.!?])\s+(?=[A-Z\[])|(?<=\])\s+(?=[A-Z])", normalized)
    return [sentence.strip() for sentence in sentences if sentence.strip()]


def expand_numeric_token(token: str) -> list[int]:
    token = token.strip().replace("–", "-").replace("—", "-")
    if not token:
        return []
    if "-" in token:
        start_text, end_text = token.split("-", 1)
        if start_text.strip().isdigit() and end_text.strip().isdigit():
            start = int(start_text.strip())
            end = int(end_text.strip())
            if start <= end:
                return list(range(start, end + 1))
    if token.isdigit():
        return [int(token)]
    return []


def extract_citation_ids_from_sentence(sentence: str) -> list[int]:
    citation_ids: list[int] = []
    for content in BODY_CITATION_PATTERN.findall(sentence):
        for token in content.split(","):
            citation_ids.extend(expand_numeric_token(token))
    return sorted(set(citation_ids))


def extract_citation_contexts(body_text: str, citation_ids: set[int]) -> dict[int, list[dict[str, Any]]]:
    sentences = split_into_sentences(body_text)
    contexts_by_citation: dict[int, list[dict[str, Any]]] = {citation_id: [] for citation_id in citation_ids}

    for index, sentence in enumerate(sentences):
        sentence_ids = extract_citation_ids_from_sentence(sentence)
        if not sentence_ids:
            continue

        expanded_context = []
        if index > 0:
            expanded_context.append(sentences[index - 1])
        expanded_context.append(sentence)
        if index + 1 < len(sentences):
            expanded_context.append(sentences[index + 1])

        context_obj = {
            "sentence": sentence,
            "expanded_context": expanded_context,
        }
        for citation_id in sentence_ids:
            if citation_id in contexts_by_citation and context_obj not in contexts_by_citation[citation_id]:
                contexts_by_citation[citation_id].append(context_obj)

    return contexts_by_citation


def save_json(output_path: str, data: list[dict[str, Any]]) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    print(f"[parse_agent] Wrote JSON to: {path}")


def resolve_output_path(output_path: str, tag: str | None) -> str:
    if not tag:
        return output_path
    path = Path(output_path)
    return str(path.with_name(f"{path.stem}_{tag}{path.suffix}"))


def build_output_records(parsed_rows: list[dict[str, str]], contexts_by_citation: dict[int, list[dict[str, Any]]], source_pdf: str) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in parsed_rows:
        citation_id = int(row["reference_id"])
        authors = [part.strip() for part in row.get("authors", "").split(";") if part.strip()]
        doi = row.get("doi") or None
        url = None
        arxiv_id = row.get("arxiv_id") or None
        if arxiv_id:
            url = f"https://arxiv.org/abs/{arxiv_id}"
        output.append(
            {
                "citation_id": citation_id,
                "raw_citation": row.get("raw_citation") or None,
                "parsed_title": row.get("title") or None,
                "parsed_authors": authors,
                "parsed_year": extract_year(row.get("raw_citation", "")),
                "parsed_venue": row.get("venue") or None,
                "parsed_doi": doi,
                "parsed_url": url,
                "parsed_arxiv_id": arxiv_id,
                "contexts": contexts_by_citation.get(citation_id, []),
                "source_pdf": source_pdf,
            }
        )
    return output


def extract_year(text: str) -> int | None:
    match = re.search(r"\b((?:19|20)\d{2})\b", str(text or ""))
    return int(match.group(1)) if match else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract parsed citations and contexts from a PDF.")
    parser.add_argument("--input", default=DEFAULT_INPUT_PATH, help="Path to input PDF")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Path to output JSON")
    parser.add_argument("--output-name-tag", default=None, help="Optional tag appended to output filename")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = resolve_output_path(args.output, args.output_name_tag)
    _, full_text = extract_text_by_page(args.input)
    body_text, _ = split_document_sections(full_text)
    parsed_rows = parse_references_from_text(full_text, args.input)
    citation_ids = {int(row["reference_id"]) for row in parsed_rows}
    contexts_by_citation = extract_citation_contexts(body_text, citation_ids)
    output_records = build_output_records(parsed_rows, contexts_by_citation, args.input)
    save_json(output_path, output_records)
    print(f"[parse_agent] Done. Parsed {len(output_records)} citations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

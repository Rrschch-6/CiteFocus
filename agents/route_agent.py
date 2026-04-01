"""Rule-based router for CiteFocus local databases."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


DEFAULT_INPUT_PATH = "/home/sascha/refcheck/CiteFocus/outputs/parsed_citations.json"
DEFAULT_OUTPUT_PATH = "/home/sascha/refcheck/CiteFocus/outputs/route_plan.json"

ARXIV_KEYWORDS = {"arxiv", "arxiv preprint", "arxiv e-prints"}
NLP_KEYWORDS = {"acl", "emnlp", "naacl", "eacl", "coling", "tacl", "computational linguistics"}
CS_KEYWORDS = {
    "neurips", "nips", "icml", "iclr", "aaai", "ijcai", "kdd", "www", "sigir",
    "sigmod", "vldb", "cvpr", "iccv", "eccv", "usenix", "ndss", "oakland", "ccs",
    "transformer", "language model", "large language model", "neural", "machine learning",
    "deep learning", "security", "attack", "benchmark", "vision",
}
BIOMED_KEYWORDS = {
    "clinical", "medicine", "medical", "oncology", "genetics", "genome", "biology",
    "bioinformatics", "cell", "cancer", "protein", "patient", "pmid", "pmc",
    "disease", "therapy", "diagnosis", "biomarker",
}


def load_json(path: str) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError("Router input must be a JSON list.")
    return data


def save_json(path: str, data: list[dict[str, Any]]) -> None:
    output_file = Path(path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    print(f"[route_agent] Wrote JSON to: {output_file}")


def normalize_text(value: Any) -> str:
    return " ".join(str(value or "").lower().split())


def contains_any(text: str, keywords: set[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def is_arxiv_like(record: dict[str, Any]) -> bool:
    return bool(record.get("parsed_arxiv_id")) or "arxiv.org" in normalize_text(record.get("parsed_url")) or contains_any(
        " ".join([normalize_text(record.get("parsed_venue")), normalize_text(record.get("raw_citation"))]),
        ARXIV_KEYWORDS,
    )


def is_nlp_like(record: dict[str, Any]) -> bool:
    combined = " ".join([
        normalize_text(record.get("parsed_venue")),
        normalize_text(record.get("parsed_title")),
        normalize_text(record.get("raw_citation")),
    ])
    return contains_any(combined, NLP_KEYWORDS)


def is_cs_like(record: dict[str, Any]) -> bool:
    combined = " ".join([
        normalize_text(record.get("parsed_venue")),
        normalize_text(record.get("parsed_title")),
        normalize_text(record.get("raw_citation")),
    ])
    return contains_any(combined, CS_KEYWORDS) or is_nlp_like(record)


def is_biomed_like(record: dict[str, Any]) -> bool:
    combined = " ".join([
        normalize_text(record.get("parsed_venue")),
        normalize_text(record.get("parsed_title")),
        normalize_text(record.get("raw_citation")),
    ])
    return contains_any(combined, BIOMED_KEYWORDS)


def build_route_plan(record: dict[str, Any]) -> dict[str, Any]:
    parsed_doi = bool(record.get("parsed_doi"))
    arxiv_like = is_arxiv_like(record)
    nlp_like = is_nlp_like(record)
    cs_like = is_cs_like(record)
    biomed_like = is_biomed_like(record)

    if arxiv_like:
        plan = {"db_priority": ["arxiv", "openalex", "dblp"], "run_exact_match": True, "run_lexical_retrieval": True, "run_dense_retrieval": True, "confidence": 0.95}
    elif parsed_doi and (nlp_like or cs_like):
        plan = {"db_priority": ["dblp", "openalex", "arxiv"], "run_exact_match": True, "run_lexical_retrieval": True, "run_dense_retrieval": True, "confidence": 0.90}
    elif parsed_doi and biomed_like:
        plan = {"db_priority": ["openalex", "arxiv", "dblp"], "run_exact_match": True, "run_lexical_retrieval": True, "run_dense_retrieval": True, "confidence": 0.90}
    elif nlp_like or cs_like:
        plan = {"db_priority": ["dblp", "openalex", "arxiv"], "run_exact_match": parsed_doi, "run_lexical_retrieval": True, "run_dense_retrieval": True, "confidence": 0.82 if nlp_like else 0.78}
    elif biomed_like:
        plan = {"db_priority": ["openalex", "arxiv", "dblp"], "run_exact_match": parsed_doi, "run_lexical_retrieval": True, "run_dense_retrieval": True, "confidence": 0.78}
    else:
        plan = {"db_priority": ["openalex", "dblp", "arxiv"], "run_exact_match": parsed_doi, "run_lexical_retrieval": True, "run_dense_retrieval": True, "confidence": 0.55}

    return {"citation_id": record.get("citation_id"), **plan}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Route parsed citations to local retrieval databases.")
    parser.add_argument("--input", default=DEFAULT_INPUT_PATH, help="Path to parsed_citations.json")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_PATH, help="Path to route_plan.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    parsed_records = load_json(args.input)
    route_plan = [build_route_plan(record) for record in parsed_records]
    save_json(args.output, route_plan)
    print(f"[route_agent] Done. Routed {len(route_plan)} citations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

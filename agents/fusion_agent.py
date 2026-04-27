"""Fusion agent for CiteFocus."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


DEFAULT_PARSED_PATH = "/home/sascha/refcheck/CiteFocus/outputs/parsed_citations.json"
DEFAULT_ROUTE_PATH = "/home/sascha/refcheck/CiteFocus/outputs/route_plan.json"
DEFAULT_EXACT_PATH = "/home/sascha/refcheck/CiteFocus/outputs/exact_matches.json"
DEFAULT_LEXICAL_PATH = "/home/sascha/refcheck/CiteFocus/outputs/lexical_candidates.json"
DEFAULT_STAGE1_OUTPUT_PATH = "/home/sascha/refcheck/CiteFocus/outputs/fused_candidates_stage1.json"
DEFAULT_STAGE2_OUTPUT_PATH = "/home/sascha/refcheck/CiteFocus/outputs/fused_candidates_stage2.json"

HIGH_CONFIDENCE_EXACT_TYPES = {"arxiv_id_exact", "doi_exact"}
LEXICAL_BACKUP_SCORE_GAP = 0.05


def load_json(path: str) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON list in {path}")
    return data


def load_json_if_exists(path: str | None) -> list[dict[str, Any]]:
    if not path:
        return []
    input_path = Path(path)
    if not input_path.exists():
        return []
    return load_json(path)


def save_json(path: str, data: list[dict[str, Any]]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    print(f"[fusion_agent] Wrote JSON to: {output_path}")


def build_record_map(records: list[dict[str, Any]]) -> dict[Any, dict[str, Any]]:
    return {record.get("citation_id"): record for record in records}


def lexical_summary_for_record(lexical_record: dict[str, Any] | None) -> dict[str, Any]:
    lexical_candidates = (lexical_record or {}).get("candidates") or []
    return {
        "run_lexical_retrieval": (lexical_record or {}).get("run_lexical_retrieval"),
        "skipped": (lexical_record or {}).get("skipped"),
        "skip_reason": (lexical_record or {}).get("skip_reason"),
        "db_timings_ms": (lexical_record or {}).get("db_timings_ms", {}),
        "candidate_count": len(lexical_candidates),
    }


def lexical_backups_if_close(lexical_candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(lexical_candidates) < 2:
        return []
    top = lexical_candidates[0]
    second = lexical_candidates[1]
    top_score = float(top.get("lexical_score") or 0.0)
    second_score = float(second.get("lexical_score") or 0.0)
    if top_score - second_score <= LEXICAL_BACKUP_SCORE_GAP:
        return [second]
    return []


def make_stage1_result(citation: dict[str, Any], route_record: dict[str, Any], exact_record: dict[str, Any] | None) -> dict[str, Any]:
    citation_id = citation.get("citation_id")
    selected_source = None
    selected_candidate = None
    selected_confidence = 0.0
    selected_match_type = None
    fusion_status = "no_candidate"

    if exact_record and exact_record.get("match_found") and exact_record.get("matched_record"):
        selected_source = "exact"
        selected_candidate = exact_record["matched_record"]
        selected_confidence = float(exact_record.get("confidence") or 0.0)
        selected_match_type = exact_record.get("match_type")
        fusion_status = "high_confidence_exact" if selected_match_type in HIGH_CONFIDENCE_EXACT_TYPES else "exact"

    return {
        "citation_id": citation_id,
        "source_pdf": citation.get("source_pdf"),
        "db_priority": route_record.get("db_priority", []),
        "effective_db_priority": [],
        "selected_source": selected_source,
        "selected_match_type": selected_match_type,
        "selected_confidence": round(selected_confidence, 4),
        "fusion_status": fusion_status,
        "selected_candidate": selected_candidate,
        "backup_candidates": [],
        "exact_summary": {
            "run_exact_match": (exact_record or {}).get("run_exact_match"),
            "match_found": (exact_record or {}).get("match_found"),
            "match_type": (exact_record or {}).get("match_type"),
            "matched_db": (exact_record or {}).get("matched_db"),
            "confidence": (exact_record or {}).get("confidence"),
        },
        "lexical_summary": {
            "run_lexical_retrieval": None,
            "skipped": None,
            "skip_reason": None,
            "db_timings_ms": {},
            "candidate_count": 0,
        },
    }


def make_stage2_result(
    citation: dict[str, Any],
    route_record: dict[str, Any],
    exact_record: dict[str, Any] | None,
    lexical_record: dict[str, Any] | None,
) -> dict[str, Any]:
    citation_id = citation.get("citation_id")
    lexical_candidates = (lexical_record or {}).get("candidates") or []
    lexical_top = lexical_candidates[0] if lexical_candidates else None
    selected_source = None
    selected_candidate = None
    selected_confidence = 0.0
    selected_match_type = None
    fusion_status = "no_candidate"
    backup_candidates: list[dict[str, Any]] = []

    if exact_record and exact_record.get("match_found") and exact_record.get("matched_record"):
        selected_source = "exact"
        selected_candidate = exact_record["matched_record"]
        selected_confidence = float(exact_record.get("confidence") or 0.0)
        selected_match_type = exact_record.get("match_type")
        fusion_status = "high_confidence_exact" if selected_match_type in HIGH_CONFIDENCE_EXACT_TYPES else "exact"
        backup_candidates = []
    elif lexical_top is not None:
        selected_source = "lexical"
        selected_candidate = lexical_top
        selected_confidence = float(lexical_top.get("lexical_score") or 0.0)
        selected_match_type = "lexical_top_1"
        fusion_status = "lexical"
        backup_candidates = lexical_backups_if_close(lexical_candidates)

    return {
        "citation_id": citation_id,
        "source_pdf": citation.get("source_pdf"),
        "db_priority": route_record.get("db_priority", []),
        "effective_db_priority": (lexical_record or {}).get("effective_db_priority", []),
        "selected_source": selected_source,
        "selected_match_type": selected_match_type,
        "selected_confidence": round(selected_confidence, 4),
        "fusion_status": fusion_status,
        "selected_candidate": selected_candidate,
        "backup_candidates": backup_candidates,
        "exact_summary": {
            "run_exact_match": (exact_record or {}).get("run_exact_match"),
            "match_found": (exact_record or {}).get("match_found"),
            "match_type": (exact_record or {}).get("match_type"),
            "matched_db": (exact_record or {}).get("matched_db"),
            "confidence": (exact_record or {}).get("confidence"),
        },
        "lexical_summary": lexical_summary_for_record(lexical_record),
    }


def default_output_for_stage(stage: str) -> str:
    if stage == "stage1":
        return DEFAULT_STAGE1_OUTPUT_PATH
    return DEFAULT_STAGE2_OUTPUT_PATH


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fuse CiteFocus exact and lexical results.")
    parser.add_argument("--stage", choices=["stage1", "stage2"], required=True, help="Fusion stage to run.")
    parser.add_argument("--parsed", default=DEFAULT_PARSED_PATH, help="Path to parsed_citations.json")
    parser.add_argument("--route", default=DEFAULT_ROUTE_PATH, help="Path to route_plan.json")
    parser.add_argument("--exact", default=DEFAULT_EXACT_PATH, help="Path to exact_matches.json")
    parser.add_argument("--lexical", default=DEFAULT_LEXICAL_PATH, help="Path to lexical_candidates.json")
    parser.add_argument("--output", default=None, help="Path to fused JSON output")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    parsed_records = load_json(args.parsed)
    route_map = build_record_map(load_json(args.route))
    exact_map = build_record_map(load_json(args.exact))
    lexical_map = build_record_map(load_json_if_exists(args.lexical)) if args.stage == "stage2" else {}
    results = []

    for citation in parsed_records:
        citation_id = citation.get("citation_id")
        route_record = route_map.get(
            citation_id,
            {
                "citation_id": citation_id,
                "db_priority": [],
                "run_exact_match": False,
                "run_lexical_retrieval": False,
                "run_dense_retrieval": False,
                "confidence": 0.0,
            },
        )
        exact_record = exact_map.get(citation_id)
        if args.stage == "stage1":
            result = make_stage1_result(citation, route_record, exact_record)
        else:
            lexical_record = lexical_map.get(citation_id)
            result = make_stage2_result(citation, route_record, exact_record, lexical_record)
        print(
            f"[fusion_agent] stage={args.stage} citation_id={citation_id} "
            f"fusion_status={result['fusion_status']} selected_source={result['selected_source']}"
        )
        results.append(result)

    output_path = args.output or default_output_for_stage(args.stage)
    save_json(output_path, results)
    print(f"[fusion_agent] Done. Processed {len(results)} citations.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

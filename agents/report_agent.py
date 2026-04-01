"""Reporting agent for CiteFocus."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt


DEFAULT_VERIFICATION_PATH = "/home/sascha/refcheck/CiteFocus/outputs/verification_results.json"
DEFAULT_SEMANTIC_PATH = "/home/sascha/refcheck/CiteFocus/outputs/semantic_results.json"
DEFAULT_SUMMARY_PATH = "/home/sascha/refcheck/CiteFocus/outputs/report_summary.json"
DEFAULT_COMBINED_PATH = "/home/sascha/refcheck/CiteFocus/outputs/report_combined.json"
DEFAULT_COMBINED_CSV_PATH = "/home/sascha/refcheck/CiteFocus/outputs/report_combined.csv"
DEFAULT_REVIEW_QUEUE_PATH = "/home/sascha/refcheck/CiteFocus/outputs/report_review_queue.json"
DEFAULT_SOURCE_SUMMARY_PATH = "/home/sascha/refcheck/CiteFocus/outputs/report_source_summary.json"
DEFAULT_CHART_DIR = "/home/sascha/refcheck/CiteFocus/outputs"

VERIFICATION_ORDER = ["verified", "partially_verified", "ambiguous", "hallucinated"]
SEMANTIC_ORDER = ["supported", "partially_supported", "unclear", "unsupported", "skipped"]


def load_json(path: str) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON list in {path}")
    return data


def save_json(path: str, data: Any) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, indent=2)
    print(f"[report_agent] Wrote JSON to: {output_path}")


def save_csv(path: str, rows: list[dict[str, Any]]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "citation_id",
        "verification_status",
        "verification_category",
        "verification_subcategory",
        "author_status",
        "year_status",
        "verification_confidence",
        "bibliographic_score",
        "semantic_label",
        "semantic_score",
        "semantic_skipped",
        "semantic_skip_reason",
        "selected_source",
        "selected_match_type",
        "selected_candidate_db",
        "selected_candidate_title",
        "verification_explanation",
        "semantic_explanation",
        "combined_label",
        "needs_review",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})
    print(f"[report_agent] Wrote CSV to: {output_path}")


def build_record_map(records: list[dict[str, Any]]) -> dict[Any, dict[str, Any]]:
    return {record.get("citation_id"): record for record in records}


def normalize_semantic_label(record: dict[str, Any] | None) -> str:
    if not record:
        return "skipped"
    if record.get("skipped"):
        return "skipped"
    label = str(record.get("support_label") or "unclear").strip().lower()
    if label not in {"supported", "partially_supported", "unclear", "unsupported"}:
        return "unclear"
    return label


def determine_combined_label(verification_status: str, semantic_label: str, semantic_skipped: bool) -> str:
    if verification_status == "verified" and semantic_label in {"supported", "partially_supported"}:
        return "strongly_supported_citation"
    if verification_status == "verified" and semantic_label == "unsupported":
        return "bibliographically_valid_but_semantically_unsupported"
    if verification_status == "hallucinated":
        return "likely_problematic"
    if verification_status == "ambiguous" or semantic_skipped or semantic_label in {"unclear", "unsupported"}:
        return "needs_manual_review"
    if verification_status == "partially_verified" and semantic_label in {"supported", "partially_supported"}:
        return "promising_but_needs_review"
    return "mixed_evidence"


def merge_reports(verification_records: list[dict[str, Any]], semantic_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    semantic_map = build_record_map(semantic_records)
    combined: list[dict[str, Any]] = []
    for verification in verification_records:
        citation_id = verification.get("citation_id")
        semantic = semantic_map.get(citation_id)
        field_verification = verification.get("field_verification") or {}
        semantic_label = normalize_semantic_label(semantic)
        semantic_skipped = bool((semantic or {}).get("skipped", True))
        combined_label = determine_combined_label(
            str(verification.get("overall_status") or "ambiguous"),
            semantic_label,
            semantic_skipped,
        )
        needs_review = combined_label in {
            "needs_manual_review",
            "promising_but_needs_review",
            "bibliographically_valid_but_semantically_unsupported",
            "likely_problematic",
        }
        combined.append(
            {
                "citation_id": citation_id,
                "verification_status": verification.get("overall_status"),
                "verification_category": verification.get("verification_category"),
                "verification_subcategory": verification.get("verification_subcategory"),
                "author_status": (
                    "match"
                    if field_verification.get("author_match") == "exact"
                    else "partial"
                    if field_verification.get("author_match") == "partial"
                    else "mismatch"
                ),
                "year_status": (
                    "match"
                    if field_verification.get("year_match") == "exact"
                    else "partial"
                    if field_verification.get("year_match") == "partial"
                    else "mismatch"
                ),
                "verification_confidence": verification.get("overall_confidence"),
                "bibliographic_score": field_verification.get("bibliographic_score"),
                "semantic_label": semantic_label,
                "semantic_score": (semantic or {}).get("support_score", 0.0),
                "semantic_skipped": semantic_skipped,
                "semantic_skip_reason": (semantic or {}).get("skip_reason"),
                "selected_source": verification.get("selected_source"),
                "selected_match_type": verification.get("selected_match_type"),
                "selected_candidate_db": verification.get("selected_candidate_db"),
                "selected_candidate_title": verification.get("selected_candidate_title"),
                "verification_explanation": verification.get("explanation"),
                "semantic_explanation": (semantic or {}).get("explanation"),
                "combined_label": combined_label,
                "needs_review": needs_review,
            }
        )
    return combined


def count_with_order(values: list[str], order: list[str]) -> dict[str, int]:
    counter = Counter(values)
    return {label: counter.get(label, 0) for label in order}


def percentages_from_counts(counts: dict[str, int], total: int) -> dict[str, float]:
    if total <= 0:
        return {label: 0.0 for label in counts}
    return {label: round((count / total) * 100.0, 2) for label, count in counts.items()}


def build_summary(combined_rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(combined_rows)
    verification_counts = count_with_order([str(row.get("verification_status")) for row in combined_rows], VERIFICATION_ORDER)
    verification_category_order = ["Verified", "Partially Verified", "Not Verified"]
    verification_category_counts = count_with_order([str(row.get("verification_category")) for row in combined_rows], verification_category_order)
    verification_subcategories: dict[str, dict[str, int]] = {}
    for category in verification_category_order:
        category_rows = [row for row in combined_rows if row.get("verification_category") == category]
        sub_counts = Counter(str(row.get("verification_subcategory") or "unknown") for row in category_rows)
        verification_subcategories[category] = dict(sorted(sub_counts.items()))
    semantic_counts = count_with_order([str(row.get("semantic_label")) for row in combined_rows], SEMANTIC_ORDER)
    combined_counts = dict(Counter(str(row.get("combined_label")) for row in combined_rows))
    crosstab: dict[str, dict[str, int]] = {}
    for verification_label in VERIFICATION_ORDER:
        row_counts = defaultdict(int)
        for row in combined_rows:
            if row.get("verification_status") == verification_label:
                row_counts[str(row.get("semantic_label"))] += 1
        crosstab[verification_label] = {label: row_counts.get(label, 0) for label in SEMANTIC_ORDER}
    return {
        "total_citations": total,
        "verification": {
            "counts": verification_counts,
            "percentages": percentages_from_counts(verification_counts, total),
            "category_counts": verification_category_counts,
            "category_percentages": percentages_from_counts(verification_category_counts, total),
            "subcategory_counts": verification_subcategories,
        },
        "semantic": {
            "counts": semantic_counts,
            "percentages": percentages_from_counts(semantic_counts, total),
            "supporting_count": semantic_counts["supported"] + semantic_counts["partially_supported"],
            "not_supporting_count": semantic_counts["unsupported"],
            "semantically_verified_count": semantic_counts["supported"] + semantic_counts["partially_supported"],
        },
        "combined_labels": {
            "counts": combined_counts,
            "percentages": percentages_from_counts(combined_counts, total),
        },
        "verification_semantic_crosstab": crosstab,
        "needs_review_count": sum(1 for row in combined_rows if row.get("needs_review")),
        "lexically_verified_count": sum(1 for row in combined_rows if row.get("selected_source") == "lexical"),
        "not_lexically_verified_count": sum(1 for row in combined_rows if row.get("selected_source") != "lexical"),
    }


def build_review_queue(combined_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    review = [row for row in combined_rows if row.get("needs_review")]
    review.sort(
        key=lambda row: (
            row.get("verification_status") != "hallucinated",
            row.get("verification_status") != "ambiguous",
            row.get("semantic_label") != "unsupported",
            -(float(row.get("verification_confidence") or 0.0)),
        )
    )
    return review


def build_source_summary(combined_rows: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in combined_rows:
        grouped[str(row.get("selected_candidate_db") or "unknown")].append(row)

    summary: dict[str, Any] = {}
    for source, rows in grouped.items():
        verification_counts = dict(Counter(str(row.get("verification_status")) for row in rows))
        semantic_counts = dict(Counter(str(row.get("semantic_label")) for row in rows))
        summary[source] = {
            "count": len(rows),
            "average_bibliographic_score": round(
                sum(float(row.get("bibliographic_score") or 0.0) for row in rows) / max(1, len(rows)),
                4,
            ),
            "average_semantic_score": round(
                sum(float(row.get("semantic_score") or 0.0) for row in rows if not row.get("semantic_skipped"))
                / max(1, sum(1 for row in rows if not row.get("semantic_skipped"))),
                4,
            ),
            "verification_counts": verification_counts,
            "semantic_counts": semantic_counts,
        }
    return summary


def make_bar_chart(labels: list[str], values: list[int], title: str, ylabel: str, path: Path, color: str) -> None:
    plt.figure(figsize=(9, 5))
    bars = plt.bar(labels, values, color=color)
    plt.title(title)
    plt.ylabel(ylabel)
    plt.xticks(rotation=20, ha="right")
    for bar, value in zip(bars, values):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), str(value), ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()
    print(f"[report_agent] Wrote chart to: {path}")


def make_crosstab_heatmap(summary: dict[str, Any], path: Path) -> None:
    data = summary["verification_semantic_crosstab"]
    rows = VERIFICATION_ORDER
    cols = SEMANTIC_ORDER
    matrix = [[data.get(row, {}).get(col, 0) for col in cols] for row in rows]
    plt.figure(figsize=(9, 5))
    plt.imshow(matrix, cmap="Blues", aspect="auto")
    plt.colorbar(label="Count")
    plt.xticks(range(len(cols)), cols, rotation=20, ha="right")
    plt.yticks(range(len(rows)), rows)
    plt.title("Verification vs Semantic Crosstab")
    for i, row in enumerate(rows):
        for j, col in enumerate(cols):
            plt.text(j, i, str(matrix[i][j]), ha="center", va="center", color="black", fontsize=9)
    plt.tight_layout()
    plt.savefig(path, dpi=180)
    plt.close()
    print(f"[report_agent] Wrote chart to: {path}")


def write_charts(summary: dict[str, Any], source_summary: dict[str, Any], chart_dir: Path, tag: str | None = None) -> list[str]:
    chart_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"_{tag}" if tag else ""
    written: list[str] = []

    verification_chart = chart_dir / f"chart_verification_status{suffix}.png"
    make_bar_chart(
        VERIFICATION_ORDER,
        [summary["verification"]["counts"][label] for label in VERIFICATION_ORDER],
        "Verification Status Distribution",
        "Citations",
        verification_chart,
        "#4C72B0",
    )
    written.append(str(verification_chart))

    semantic_chart = chart_dir / f"chart_semantic_support{suffix}.png"
    make_bar_chart(
        SEMANTIC_ORDER,
        [summary["semantic"]["counts"][label] for label in SEMANTIC_ORDER],
        "Semantic Support Distribution",
        "Citations",
        semantic_chart,
        "#55A868",
    )
    written.append(str(semantic_chart))

    crosstab_chart = chart_dir / f"chart_verification_semantic_crosstab{suffix}.png"
    make_crosstab_heatmap(summary, crosstab_chart)
    written.append(str(crosstab_chart))

    source_labels = sorted(source_summary.keys())
    source_chart = chart_dir / f"chart_selected_source{suffix}.png"
    make_bar_chart(
        source_labels,
        [int(source_summary[source]["count"]) for source in source_labels],
        "Selected Candidate Source Distribution",
        "Citations",
        source_chart,
        "#C44E52",
    )
    written.append(str(source_chart))
    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create final CiteFocus reports and charts.")
    parser.add_argument("--verification", default=DEFAULT_VERIFICATION_PATH, help="Path to verification_results.json")
    parser.add_argument("--semantic", default=DEFAULT_SEMANTIC_PATH, help="Path to semantic_results.json")
    parser.add_argument("--summary-output", default=DEFAULT_SUMMARY_PATH, help="Path to summary JSON")
    parser.add_argument("--combined-output", default=DEFAULT_COMBINED_PATH, help="Path to combined per-citation JSON")
    parser.add_argument("--combined-csv-output", default=DEFAULT_COMBINED_CSV_PATH, help="Path to combined CSV")
    parser.add_argument("--review-output", default=DEFAULT_REVIEW_QUEUE_PATH, help="Path to review queue JSON")
    parser.add_argument("--source-summary-output", default=DEFAULT_SOURCE_SUMMARY_PATH, help="Path to source summary JSON")
    parser.add_argument("--chart-dir", default=DEFAULT_CHART_DIR, help="Directory for saved charts")
    parser.add_argument("--tag", default=None, help="Optional suffix tag for chart names")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    verification_records = load_json(args.verification)
    semantic_records = load_json(args.semantic)

    combined_rows = merge_reports(verification_records, semantic_records)
    summary = build_summary(combined_rows)
    review_queue = build_review_queue(combined_rows)
    source_summary = build_source_summary(combined_rows)
    chart_paths = write_charts(summary, source_summary, Path(args.chart_dir), tag=args.tag)

    summary["chart_paths"] = chart_paths

    save_json(args.combined_output, combined_rows)
    save_csv(args.combined_csv_output, combined_rows)
    save_json(args.summary_output, summary)
    save_json(args.review_output, review_queue)
    save_json(args.source_summary_output, source_summary)

    print(
        "[report_agent] summary:"
        f" total={summary['total_citations']}"
        f" verified={summary['verification']['counts']['verified']}"
        f" supported={summary['semantic']['counts']['supported']}"
        f" unsupported={summary['semantic']['counts']['unsupported']}"
        f" needs_review={summary['needs_review_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

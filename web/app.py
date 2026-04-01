from __future__ import annotations

import json
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from flask import Flask, abort, flash, jsonify, redirect, render_template, request, send_from_directory, url_for


ROOT = Path("/home/sascha/refcheck/CiteFocus")
OUTPUTS_DIR = ROOT / "outputs"
UPLOADS_DIR = ROOT / "web" / "uploads"
PIPELINE_SCRIPT = ROOT / "run_pipeline.py"
LOGO_PATH = ROOT / "web" / "Logo_SSL_Colored.png"
JOB_LOCK = threading.Lock()
JOBS: dict[str, dict[str, Any]] = {}
INTERNAL_STAGE_KEYS = [
    "parse",
    "route",
    "exact",
    "fusion_stage1",
    "lexical",
    "fusion_stage2",
    "verify",
    "semantic_stage1",
    "semantic_stage2",
    "report",
]
DISPLAY_STAGE_ORDER = ["parse", "route", "exact", "lexical", "verify", "semantic", "report"]


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=str(ROOT / "web" / "templates"),
        static_folder=str(ROOT / "web" / "static"),
    )
    app.config["MAX_CONTENT_LENGTH"] = 100 * 1024 * 1024
    app.secret_key = "citefocus-local-dev"
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

    @app.route("/")
    def index():
        recent_runs = list_recent_runs()
        return render_template("index.html", recent_runs=recent_runs)

    @app.post("/run")
    def run_pipeline_route():
        uploaded = request.files.get("pdf")
        if uploaded is None or not uploaded.filename:
            flash("Choose a PDF to upload.", "error")
            return redirect(url_for("index"))

        filename = uploaded.filename
        if not filename.lower().endswith(".pdf"):
            flash("Only PDF uploads are supported.", "error")
            return redirect(url_for("index"))

        tag = make_run_tag(Path(filename).stem)
        saved_pdf = UPLOADS_DIR / f"{tag}.pdf"
        uploaded.save(saved_pdf)
        init_job(tag, saved_pdf)

        worker = threading.Thread(target=background_run_pipeline, args=(tag, saved_pdf), daemon=True)
        worker.start()
        return redirect(url_for("run_status_view", tag=tag))

    @app.route("/runs/<tag>")
    def run_status_view(tag: str):
        job = get_job(tag)
        if job is None:
            abort(404)
        return render_template("run_status.html", tag=tag, stage_order=DISPLAY_STAGE_ORDER)

    @app.route("/runs/<tag>/status")
    def run_status_api(tag: str):
        job = get_job(tag)
        if job is None:
            abort(404)
        payload = dict(job)
        payload["display_stages"] = build_display_stages(job)
        payload["total_elapsed_seconds"] = current_total_elapsed(job)
        payload["report_url"] = url_for("report_view", tag=tag) if job.get("status") == "completed" else None
        return jsonify(payload)

    @app.route("/reports/<tag>")
    def report_view(tag: str):
        summary = load_optional_json(OUTPUTS_DIR / f"report_summary_{tag}.json")
        combined = load_optional_json(OUTPUTS_DIR / f"report_combined_{tag}.json") or []
        review_queue = load_optional_json(OUTPUTS_DIR / f"report_review_queue_{tag}.json") or []
        source_summary = load_optional_json(OUTPUTS_DIR / f"report_source_summary_{tag}.json") or {}

        if summary is None:
            abort(404)

        chart_files = [
            f"chart_verification_status_{tag}.png",
            f"chart_semantic_support_{tag}.png",
            f"chart_verification_semantic_crosstab_{tag}.png",
            f"chart_selected_source_{tag}.png",
        ]
        stage_metrics = collect_stage_metrics(tag)

        return render_template(
            "report.html",
            tag=tag,
            summary=summary,
            combined=combined,
            review_queue=review_queue,
            source_summary=source_summary,
            chart_files=chart_files,
            stage_metrics=stage_metrics,
        )

    @app.route("/artifacts/<path:filename>")
    def artifact(filename: str):
        return send_from_directory(OUTPUTS_DIR, filename)

    @app.route("/logo")
    def logo():
        return send_from_directory(LOGO_PATH.parent, LOGO_PATH.name)

    return app


def make_run_tag(stem: str) -> str:
    clean_stem = re.sub(r"[^a-zA-Z0-9]+", "_", stem).strip("_").lower() or "pdf"
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    return f"{clean_stem}_{timestamp}"


def load_optional_json(path: Path) -> Any:
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def list_recent_runs(limit: int = 8) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for path in sorted(OUTPUTS_DIR.glob("report_summary*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        tag = path.stem.removeprefix("report_summary").lstrip("_")
        if not tag:
            continue
        data = load_optional_json(path) or {}
        runs.append(
            {
                "tag": tag,
                "total_citations": data.get("total_citations", 0),
                "needs_review_count": data.get("needs_review_count", 0),
            }
        )
        if len(runs) >= limit:
            break
    return runs


def init_job(tag: str, pdf_path: Path) -> None:
    with JOB_LOCK:
        JOBS[tag] = {
            "tag": tag,
            "pdf_path": str(pdf_path),
            "status": "queued",
            "current_stage": None,
            "started_at": time.time(),
            "finished_at": None,
            "duration_seconds": None,
            "stages": {stage: "pending" for stage in INTERNAL_STAGE_KEYS},
            "stage_started_at": {stage: None for stage in INTERNAL_STAGE_KEYS},
            "stage_durations": {stage: 0.0 for stage in INTERNAL_STAGE_KEYS},
            "log_lines": [],
            "error": None,
        }


def get_job(tag: str) -> dict[str, Any] | None:
    with JOB_LOCK:
        job = JOBS.get(tag)
        if job is None:
            return None
        return json.loads(json.dumps(job))


def append_job_log(tag: str, line: str) -> None:
    with JOB_LOCK:
        job = JOBS.get(tag)
        if job is None:
            return
        lines = job.setdefault("log_lines", [])
        lines.append(line.rstrip())
        if len(lines) > 40:
            del lines[:-40]


def set_stage_running(tag: str, stage: str) -> None:
    with JOB_LOCK:
        job = JOBS.get(tag)
        if job is None:
            return
        job["status"] = "running"
        job["current_stage"] = stage
        current_status = job["stages"].get(stage)
        if current_status != "running":
            job["stages"][stage] = "running"
            job["stage_started_at"][stage] = time.time()


def set_stage_completed(tag: str, stage: str) -> None:
    with JOB_LOCK:
        job = JOBS.get(tag)
        if job is None:
            return
        status = job["stages"].get(stage)
        started_at = job["stage_started_at"].get(stage)
        if status == "running" and started_at is not None:
            job["stage_durations"][stage] = round(time.time() - started_at, 2)
        if status != "completed":
            job["stages"][stage] = "completed"
        job["stage_started_at"][stage] = None

def mark_stage_failed(tag: str, stage: str) -> None:
    with JOB_LOCK:
        job = JOBS.get(tag)
        if job is None:
            return
        started_at = job["stage_started_at"].get(stage)
        if job["stages"].get(stage) == "running" and started_at is not None:
            job["stage_durations"][stage] = round(time.time() - started_at, 2)
        job["stages"][stage] = "failed"
        job["stage_started_at"][stage] = None


def finish_job(tag: str, *, success: bool, error: str | None = None) -> None:
    with JOB_LOCK:
        job = JOBS.get(tag)
        if job is None:
            return
        job["finished_at"] = time.time()
        job["duration_seconds"] = round(job["finished_at"] - job["started_at"], 2)
        job["status"] = "completed" if success else "failed"
        job["error"] = error
        current_stage = job.get("current_stage")
        if success and current_stage and job["stages"].get(current_stage) == "running":
            started_at = job["stage_started_at"].get(current_stage)
            if started_at is not None:
                job["stage_durations"][current_stage] = round(job["finished_at"] - started_at, 2)
            job["stages"][current_stage] = "completed"
            job["stage_started_at"][current_stage] = None
        if not success and current_stage:
            started_at = job["stage_started_at"].get(current_stage)
            if started_at is not None:
                job["stage_durations"][current_stage] = round(job["finished_at"] - started_at, 2)
            job["stages"][current_stage] = "failed"
            job["stage_started_at"][current_stage] = None


def infer_stage_from_line(line: str) -> str | None:
    if "parse_agent.py" in line:
        return "parse"
    if "route_agent.py" in line:
        return "route"
    if "exact_match_agent.py" in line:
        return "exact"
    if "fusion_agent.py" in line and "--stage stage1" in line:
        return "fusion_stage1"
    if "lexical_retrieval_agent.py" in line:
        return "lexical"
    if "fusion_agent.py" in line and "--stage stage2" in line:
        return "fusion_stage2"
    if "verify_agent.py" in line:
        return "verify"
    if "semantic_agent.py" in line and "--stage stage1" in line:
        return "semantic_stage1"
    if "semantic_agent.py" in line and "--stage stage2" in line:
        return "semantic_stage2"
    if "report_agent.py" in line:
        return "report"
    if "loading semantic model once" in line:
        return "semantic_stage1"
    return None


def handle_stage_transition(tag: str, stage: str) -> None:
    if stage == "route":
        set_stage_completed(tag, "parse")
    elif stage == "exact":
        set_stage_completed(tag, "route")
    elif stage == "fusion_stage1":
        set_stage_completed(tag, "exact")
    elif stage == "lexical":
        set_stage_completed(tag, "fusion_stage1")
    elif stage == "fusion_stage2":
        set_stage_completed(tag, "lexical")
    elif stage == "verify":
        set_stage_completed(tag, "fusion_stage2")
    elif stage == "semantic_stage2":
        set_stage_completed(tag, "semantic_stage1")
        set_stage_completed(tag, "verify")
    elif stage == "report":
        set_stage_completed(tag, "semantic_stage2")
        set_stage_completed(tag, "verify")
    set_stage_running(tag, stage)


def current_total_elapsed(job: dict[str, Any]) -> float:
    started_at = job.get("started_at")
    if not started_at:
        return 0.0
    if job.get("duration_seconds") is not None:
        return float(job["duration_seconds"])
    return round(time.time() - started_at, 2)


def _internal_stage_elapsed(job: dict[str, Any], stage: str) -> float:
    if job["stages"].get(stage) == "running":
        started_at = job["stage_started_at"].get(stage)
        if started_at is not None:
            return round(time.time() - started_at, 2)
    return float(job["stage_durations"].get(stage) or 0.0)


def build_display_stages(job: dict[str, Any]) -> dict[str, dict[str, Any]]:
    internal = job.get("stages", {})
    display: dict[str, dict[str, Any]] = {}
    for stage in ["parse", "route", "exact", "lexical", "verify", "report"]:
        status = internal.get(stage, "pending")
        display[stage] = {
            "status": status,
            "elapsed_seconds": _internal_stage_elapsed(job, stage),
        }

    semantic_statuses = [internal.get("semantic_stage1", "pending"), internal.get("semantic_stage2", "pending")]
    if "failed" in semantic_statuses:
        semantic_status = "failed"
    elif "running" in semantic_statuses:
        semantic_status = "running"
    elif any(status == "completed" for status in semantic_statuses):
        semantic_status = "completed"
    else:
        semantic_status = "pending"
    display["semantic"] = {
        "status": semantic_status,
        "elapsed_seconds": round(
            _internal_stage_elapsed(job, "semantic_stage1") + _internal_stage_elapsed(job, "semantic_stage2"),
            2,
        ),
    }
    return display


def background_run_pipeline(tag: str, pdf_path: Path) -> None:
    cmd = [
        sys.executable,
        str(PIPELINE_SCRIPT),
        "--input-pdf",
        str(pdf_path),
        "--output-name-tag",
        tag,
    ]
    log_path = OUTPUTS_DIR / f"pipeline_log_{tag}.txt"
    set_stage_running(tag, "parse")
    append_job_log(tag, f"$ {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    collected: list[str] = []
    assert process.stdout is not None
    for raw_line in process.stdout:
        line = raw_line.rstrip()
        collected.append(line)
        append_job_log(tag, line)
        if "finished: lexical" in line:
            set_stage_completed(tag, "lexical")
        if "finished: semantic_stage1" in line:
            set_stage_completed(tag, "semantic_stage1")
        stage = infer_stage_from_line(line)
        if stage:
            handle_stage_transition(tag, stage)

    return_code = process.wait()
    log_path.write_text("\n".join(collected) + "\n", encoding="utf-8")

    if return_code == 0:
        for stage in INTERNAL_STAGE_KEYS:
            if get_job(tag)["stages"].get(stage) == "running":
                set_stage_completed(tag, stage)
        finish_job(tag, success=True)
    else:
        current_stage = get_job(tag).get("current_stage")
        if current_stage:
            mark_stage_failed(tag, current_stage)
        finish_job(tag, success=False, error=f"Pipeline exited with status {return_code}")


def collect_stage_metrics(tag: str) -> list[dict[str, Any]]:
    parsed = load_optional_json(OUTPUTS_DIR / f"parsed_citations_{tag}.json") or []
    route = load_optional_json(OUTPUTS_DIR / f"route_plan_{tag}.json") or []
    exact = load_optional_json(OUTPUTS_DIR / f"exact_matches_{tag}.json") or []
    lexical = load_optional_json(OUTPUTS_DIR / f"lexical_candidates_{tag}.json") or []
    fusion1 = load_optional_json(OUTPUTS_DIR / f"fused_candidates_stage1_{tag}.json") or []
    fusion2 = load_optional_json(OUTPUTS_DIR / f"fused_candidates_stage2_{tag}.json") or []
    verify = load_optional_json(OUTPUTS_DIR / f"verification_results_{tag}.json") or []
    semantic = load_optional_json(OUTPUTS_DIR / f"semantic_results_{tag}.json") or []
    report_summary = load_optional_json(OUTPUTS_DIR / f"report_summary_{tag}.json") or {}

    verified_count = sum(1 for item in verify if item.get("overall_status") == "verified")
    semantic_judged = sum(1 for item in semantic if not item.get("skipped"))
    exact_found = sum(1 for item in exact if item.get("match_found"))
    lexical_retrieved = sum(1 for item in lexical if not item.get("skipped"))
    fusion2_selected = sum(1 for item in fusion2 if item.get("selected_candidate"))

    return [
        {"stage": "parse", "label": "Parse", "metric": f"{len(parsed)} references found"},
        {"stage": "route", "label": "Route", "metric": f"{len(route)} citations routed"},
        {"stage": "exact", "label": "Exact", "metric": f"{exact_found}/{len(exact)} exact matches"},
        {"stage": "lexical", "label": "Lexical", "metric": f"{lexical_retrieved}/{len(lexical)} citations retrieved"},
        {"stage": "verify", "label": "Verify", "metric": f"{verified_count}/{len(verify)} verified"},
        {"stage": "semantic", "label": "Semantic", "metric": f"{semantic_judged}/{len(semantic)} semantic judgments"},
        {"stage": "report", "label": "Report", "metric": f"{report_summary.get('needs_review_count', 0)} need review"},
    ]


app = create_app()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)

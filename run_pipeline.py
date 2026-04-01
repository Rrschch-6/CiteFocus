#!/usr/bin/env python3
"""Run the CiteFocus pipeline end to end."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from agents.semantic_agent import DEFAULT_MODEL_NAME, load_model_and_tokenizer, run_semantic_stage


ROOT = Path("/home/sascha/refcheck/CiteFocus")
AGENTS_DIR = ROOT / "agents"
OUTPUTS_DIR = ROOT / "outputs"
DEFAULT_INPUT_PDF = str(ROOT / "inputs" / "nest.pdf")
PIPELINE_ORDER = ["parse", "route", "exact", "lexical", "fusion", "verify", "semantic"]


def output_path(name: str, tag: str | None) -> Path:
    suffix = f"_{tag}" if tag else ""
    return OUTPUTS_DIR / f"{name}{suffix}.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the CiteFocus agent pipeline.")
    parser.add_argument("--input-pdf", default=DEFAULT_INPUT_PDF, help="Absolute or relative path to the input PDF.")
    parser.add_argument("--output-name-tag", default=None, help="Optional tag appended to all output JSON names.")
    parser.add_argument("--agents", nargs="+", choices=PIPELINE_ORDER, default=PIPELINE_ORDER, help="Subset of agents to run in pipeline order.")
    parser.add_argument("--python", default=sys.executable, help="Python executable to use for agent scripts.")
    parser.add_argument("--semantic-model", default=DEFAULT_MODEL_NAME, help="Local Hugging Face model for semantic checks.")
    return parser.parse_args()


def normalize_agent_selection(selected: list[str]) -> list[str]:
    selected_set = set(selected)
    return [agent for agent in PIPELINE_ORDER if agent in selected_set]


def run_command(cmd: list[str]) -> None:
    print("[run_pipeline] running:", " ".join(cmd))
    subprocess.run(cmd, check=True)


def start_command(cmd: list[str]) -> subprocess.Popen[str]:
    print("[run_pipeline] starting:", " ".join(cmd))
    return subprocess.Popen(cmd)


def wait_process(process: subprocess.Popen[str], label: str) -> None:
    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, process.args)
    print(f"[run_pipeline] finished: {label}")


def main() -> int:
    start_time = time.perf_counter()
    args = parse_args()
    selected_agents = normalize_agent_selection(args.agents)
    selected_set = set(selected_agents)
    input_pdf = Path(args.input_pdf).expanduser().resolve()
    if not input_pdf.exists():
        raise FileNotFoundError(f"Input PDF not found: {input_pdf}")

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    parsed_path = output_path("parsed_citations", args.output_name_tag)
    route_path = output_path("route_plan", args.output_name_tag)
    exact_path = output_path("exact_matches", args.output_name_tag)
    lexical_path = output_path("lexical_candidates", args.output_name_tag)
    fusion_stage1_path = output_path("fused_candidates_stage1", args.output_name_tag)
    fusion_stage2_path = output_path("fused_candidates_stage2", args.output_name_tag)
    verify_path = output_path("verification_results", args.output_name_tag)
    semantic_stage1_path = output_path("semantic_results_stage1", args.output_name_tag)
    semantic_final_path = output_path("semantic_results", args.output_name_tag)

    if "parse" in selected_set:
        run_command([args.python, str(AGENTS_DIR / "parse_agent.py"), "--input", str(input_pdf), "--output", str(parsed_path)])
    if "route" in selected_set:
        run_command([args.python, str(AGENTS_DIR / "route_agent.py"), "--input", str(parsed_path), "--output", str(route_path)])
    if "exact" in selected_set:
        run_command([args.python, str(AGENTS_DIR / "exact_match_agent.py"), "--parsed", str(parsed_path), "--route", str(route_path), "--output", str(exact_path)])

    need_fusion_stage1 = bool({"fusion", "semantic", "verify"} & selected_set)
    if need_fusion_stage1:
        run_command(
            [
                args.python,
                str(AGENTS_DIR / "fusion_agent.py"),
                "--stage",
                "stage1",
                "--parsed",
                str(parsed_path),
                "--route",
                str(route_path),
                "--exact",
                str(exact_path),
                "--output",
                str(fusion_stage1_path),
            ]
        )

    lexical_process: subprocess.Popen[str] | None = None

    if "lexical" in selected_set:
        lexical_process = start_command(
            [
                args.python,
                str(AGENTS_DIR / "lexical_retrieval_agent.py"),
                "--parsed",
                str(parsed_path),
                "--route",
                str(route_path),
                "--exact",
                str(exact_path),
                "--output",
                str(lexical_path),
            ]
        )

    semantic_executor: ThreadPoolExecutor | None = None
    semantic_stage1_future = None
    semantic_model = None
    semantic_tokenizer = None
    if "semantic" in selected_set:
        print(f"[run_pipeline] loading semantic model once: {args.semantic_model}")
        semantic_model, semantic_tokenizer = load_model_and_tokenizer(args.semantic_model)
        semantic_executor = ThreadPoolExecutor(max_workers=1)
        semantic_stage1_future = semantic_executor.submit(
            run_semantic_stage,
            stage="stage1",
            parsed_path=str(parsed_path),
            fused_path=str(fusion_stage1_path),
            output_path=str(semantic_stage1_path),
            model_name=args.semantic_model,
            model=semantic_model,
            tokenizer=semantic_tokenizer,
        )

    if lexical_process is not None:
        wait_process(lexical_process, "lexical")

    need_fusion_stage2 = bool({"fusion", "semantic", "verify"} & selected_set)
    if need_fusion_stage2:
        run_command(
            [
                args.python,
                str(AGENTS_DIR / "fusion_agent.py"),
                "--stage",
                "stage2",
                "--parsed",
                str(parsed_path),
                "--route",
                str(route_path),
                "--exact",
                str(exact_path),
                "--lexical",
                str(lexical_path),
                "--output",
                str(fusion_stage2_path),
            ]
        )

    if "verify" in selected_set:
        run_command([args.python, str(AGENTS_DIR / "verify_agent.py"), "--parsed", str(parsed_path), "--fused", str(fusion_stage2_path), "--output", str(verify_path)])

    if "semantic" in selected_set:
        if semantic_stage1_future is not None:
            semantic_stage1_future.result()
            print("[run_pipeline] finished: semantic_stage1")
        run_semantic_stage(
            stage="stage2",
            parsed_path=str(parsed_path),
            fused_path=str(fusion_stage2_path),
            existing_output_path=str(semantic_stage1_path),
            output_path=str(semantic_final_path),
            model_name=args.semantic_model,
            model=semantic_model,
            tokenizer=semantic_tokenizer,
        )
        if semantic_executor is not None:
            semantic_executor.shutdown(wait=True)

    print("[run_pipeline] done")
    print("[run_pipeline] outputs:")
    print(f"  parsed:            {parsed_path}")
    print(f"  route:             {route_path}")
    print(f"  exact:             {exact_path}")
    print(f"  lexical:           {lexical_path}")
    print(f"  fusion_stage1:     {fusion_stage1_path}")
    print(f"  fusion_stage2:     {fusion_stage2_path}")
    print(f"  verify:            {verify_path}")
    print(f"  semantic_stage1:   {semantic_stage1_path}")
    print(f"  semantic_final:    {semantic_final_path}")
    elapsed_seconds = time.perf_counter() - start_time
    print(f"[run_pipeline] duration_seconds: {elapsed_seconds:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

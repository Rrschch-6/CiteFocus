"""Semantic support agent for CiteFocus."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer


DEFAULT_PARSED_PATH = "/home/sascha/refcheck/CiteFocus/outputs/parsed_citations.json"
DEFAULT_FUSED_STAGE1_PATH = "/home/sascha/refcheck/CiteFocus/outputs/fused_candidates_stage1.json"
DEFAULT_FUSED_STAGE2_PATH = "/home/sascha/refcheck/CiteFocus/outputs/fused_candidates_stage2.json"
DEFAULT_STAGE1_OUTPUT_PATH = "/home/sascha/refcheck/CiteFocus/outputs/semantic_results_stage1.json"
DEFAULT_STAGE2_OUTPUT_PATH = "/home/sascha/refcheck/CiteFocus/outputs/semantic_results.json"
DEFAULT_MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct"


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
    print(f"[semantic_agent] Wrote JSON to: {output_path}")


def build_record_map(records: list[dict[str, Any]]) -> dict[Any, dict[str, Any]]:
    return {record.get("citation_id"): record for record in records}


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def collect_expanded_context(parsed_record: dict[str, Any], max_contexts: int = 3) -> list[str]:
    contexts = parsed_record.get("contexts") or []
    collected: list[str] = []
    seen: set[str] = set()
    for context in contexts[:max_contexts]:
        expanded = context.get("expanded_context") or []
        if expanded:
            for sentence in expanded:
                sentence = normalize_space(sentence)
                if sentence and sentence not in seen:
                    seen.add(sentence)
                    collected.append(sentence)
        else:
            sentence = normalize_space(context.get("sentence"))
            if sentence and sentence not in seen:
                seen.add(sentence)
                collected.append(sentence)
    return collected


def build_semantic_prompt(parsed_record: dict[str, Any], fused_record: dict[str, Any]) -> str:
    candidate = fused_record.get("selected_candidate") or {}
    context_lines = collect_expanded_context(parsed_record)
    context_block = "\n".join(f"- {line}" for line in context_lines) if context_lines else "- No context available."
    abstract = normalize_space(candidate.get("abstract")) or "No abstract available."
    title = normalize_space(candidate.get("title")) or "Unknown title"
    return (
        "You are evaluating whether a cited paper abstract supports the cited passage context.\n"
        "Return JSON only.\n\n"
        "Citation:\n"
        f"- citation_id: {parsed_record.get('citation_id')}\n"
        f"- parsed_title: {normalize_space(parsed_record.get('parsed_title')) or 'Unknown'}\n"
        f"- raw_citation: {normalize_space(parsed_record.get('raw_citation')) or 'Unknown'}\n\n"
        "Expanded context:\n"
        f"{context_block}\n\n"
        "Selected candidate:\n"
        f"- title: {title}\n"
        f"- db: {candidate.get('db')}\n"
        f"- year: {candidate.get('year')}\n"
        f"- abstract: {abstract}\n\n"
        "Decide whether the abstract supports the citation context.\n"
        "Allowed support_label values: supported, partially_supported, unclear, unsupported.\n"
        "Return exactly this JSON schema:\n"
        "{\n"
        '  "support_label": "supported",\n'
        '  "support_score": 0.0,\n'
        '  "explanation": "short grounded explanation"\n'
        "}\n"
    )


def load_model_and_tokenizer(model_name: str):
    tokenizer = AutoTokenizer.from_pretrained(model_name, trust_remote_code=True)
    if tokenizer.pad_token_id is None and tokenizer.eos_token_id is not None:
        tokenizer.pad_token = tokenizer.eos_token
    model = AutoModelForCausalLM.from_pretrained(
        model_name,
        torch_dtype="auto",
        device_map="auto",
        trust_remote_code=True,
    )
    return model, tokenizer


def run_llm(prompt: str, model, tokenizer, max_new_tokens: int = 220) -> str:
    messages = [{"role": "user", "content": prompt}]
    if hasattr(tokenizer, "apply_chat_template"):
        inputs = tokenizer.apply_chat_template(
            messages,
            add_generation_prompt=True,
            return_tensors="pt",
            tokenize=True,
        )
        if isinstance(inputs, dict):
            model_inputs = {key: value.to(model.device) for key, value in inputs.items()}
            input_length = model_inputs["input_ids"].shape[-1]
        else:
            model_inputs = {"input_ids": inputs.to(model.device)}
            input_length = model_inputs["input_ids"].shape[-1]
    else:
        encoded = tokenizer(prompt, return_tensors="pt")
        model_inputs = {key: value.to(model.device) for key, value in encoded.items()}
        input_length = model_inputs["input_ids"].shape[-1]

    with torch.no_grad():
        outputs = model.generate(
            **model_inputs,
            max_new_tokens=max_new_tokens,
            do_sample=False,
            pad_token_id=tokenizer.pad_token_id,
            eos_token_id=tokenizer.eos_token_id,
        )
    generated = outputs[0][input_length:]
    return tokenizer.decode(generated, skip_special_tokens=True).strip()


def parse_llm_json(response_text: str) -> dict[str, Any]:
    text = response_text.strip()
    if not text:
        raise ValueError("Empty model response.")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        raise ValueError("Could not extract JSON object from model response.")
    return json.loads(match.group(0))


def make_skip_result(parsed_record: dict[str, Any], fused_record: dict[str, Any], stage: str, reason: str) -> dict[str, Any]:
    candidate = fused_record.get("selected_candidate") or {}
    return {
        "citation_id": parsed_record.get("citation_id"),
        "stage": stage,
        "selected_source": fused_record.get("selected_source"),
        "selected_candidate_title": candidate.get("title"),
        "selected_candidate_db": candidate.get("db"),
        "skipped": True,
        "skip_reason": reason,
        "support_label": "unclear",
        "support_score": 0.0,
        "explanation": reason,
    }


def verify_semantic_one(parsed_record: dict[str, Any], fused_record: dict[str, Any], *, stage: str, model, tokenizer) -> dict[str, Any]:
    candidate = fused_record.get("selected_candidate") or {}
    abstract = normalize_space(candidate.get("abstract"))
    context_lines = collect_expanded_context(parsed_record)
    if not candidate:
        return make_skip_result(parsed_record, fused_record, stage, "no_selected_candidate")
    if not abstract:
        return make_skip_result(parsed_record, fused_record, stage, "no_candidate_abstract")
    if not context_lines:
        return make_skip_result(parsed_record, fused_record, stage, "no_expanded_context")

    prompt = build_semantic_prompt(parsed_record, fused_record)
    raw_response = run_llm(prompt, model, tokenizer)
    parsed_response = parse_llm_json(raw_response)
    support_label = str(parsed_response.get("support_label") or "unclear").strip().lower()
    if support_label not in {"supported", "partially_supported", "unclear", "unsupported"}:
        support_label = "unclear"
    try:
        support_score = float(parsed_response.get("support_score", 0.0))
    except Exception:
        support_score = 0.0
    support_score = max(0.0, min(1.0, support_score))
    explanation = normalize_space(parsed_response.get("explanation")) or "No explanation returned."

    return {
        "citation_id": parsed_record.get("citation_id"),
        "stage": stage,
        "selected_source": fused_record.get("selected_source"),
        "selected_candidate_title": candidate.get("title"),
        "selected_candidate_db": candidate.get("db"),
        "skipped": False,
        "skip_reason": None,
        "support_label": support_label,
        "support_score": round(support_score, 4),
        "explanation": explanation,
    }


def default_fused_for_stage(stage: str) -> str:
    if stage == "stage1":
        return DEFAULT_FUSED_STAGE1_PATH
    return DEFAULT_FUSED_STAGE2_PATH


def default_output_for_stage(stage: str) -> str:
    if stage == "stage1":
        return DEFAULT_STAGE1_OUTPUT_PATH
    return DEFAULT_STAGE2_OUTPUT_PATH


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run semantic support checking for CiteFocus.")
    parser.add_argument("--stage", choices=["stage1", "stage2"], required=True, help="Semantic stage to run.")
    parser.add_argument("--parsed", default=DEFAULT_PARSED_PATH, help="Path to parsed_citations.json")
    parser.add_argument("--fused", default=None, help="Path to fused_candidates_stage{1,2}.json")
    parser.add_argument("--existing-output", default=None, help="Optional existing semantic results used to skip already processed citations.")
    parser.add_argument("--output", default=None, help="Path to semantic JSON output")
    parser.add_argument("--model", default=DEFAULT_MODEL_NAME, help="Local Hugging Face model name")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    fused_path = args.fused or default_fused_for_stage(args.stage)
    output_path = args.output or default_output_for_stage(args.stage)
    parsed_records = load_json(args.parsed)
    fused_map = build_record_map(load_json(fused_path))
    existing_records = load_json_if_exists(args.existing_output) if args.stage == "stage2" else []
    existing_map = build_record_map(existing_records)

    model, tokenizer = load_model_and_tokenizer(args.model)
    results = list(existing_records)
    processed_ids = set(existing_map)

    for parsed_record in parsed_records:
        citation_id = parsed_record.get("citation_id")
        if citation_id in processed_ids:
            continue
        fused_record = fused_map.get(citation_id, {"citation_id": citation_id, "selected_candidate": None, "selected_source": None})
        if args.stage == "stage1" and not fused_record.get("selected_candidate"):
            continue
        try:
            result = verify_semantic_one(parsed_record, fused_record, stage=args.stage, model=model, tokenizer=tokenizer)
        except Exception as exc:
            result = {
                "citation_id": citation_id,
                "stage": args.stage,
                "selected_source": fused_record.get("selected_source"),
                "selected_candidate_title": (fused_record.get("selected_candidate") or {}).get("title"),
                "selected_candidate_db": (fused_record.get("selected_candidate") or {}).get("db"),
                "skipped": True,
                "skip_reason": "semantic_error",
                "support_label": "unclear",
                "support_score": 0.0,
                "explanation": f"semantic_error: {exc}",
            }
        status = "skipped" if result.get("skipped") else result.get("support_label")
        print(f"[semantic_agent] stage={args.stage} citation_id={citation_id} status={status}")
        results.append(result)

    results.sort(key=lambda item: item.get("citation_id") if item.get("citation_id") is not None else -1)
    save_json(output_path, results)
    print(f"[semantic_agent] Done. Wrote {len(results)} records.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

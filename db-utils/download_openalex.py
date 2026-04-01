#!/usr/bin/env python3
"""Incrementally sync the OpenAlex works snapshot for CiteFocus."""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path


ROOT = Path("/home/sascha/refcheck/CiteFocus")
OPENALEX_DIR = ROOT / "academic_metadata" / "openalex" / "data" / "works"
DEFAULT_S3_PREFIX = "s3://openalex/data/works/"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync the OpenAlex works snapshot with aws s3 sync.")
    parser.add_argument("--s3-prefix", default=DEFAULT_S3_PREFIX, help="OpenAlex S3 prefix")
    parser.add_argument("--output-dir", default=str(OPENALEX_DIR), help="Local output directory for OpenAlex works")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    aws = shutil.which("aws")
    if aws is None:
        raise SystemExit("[download_openalex] Missing 'aws' CLI. Install awscli before running this updater.")

    cmd = [
        aws,
        "s3",
        "sync",
        "--no-sign-request",
        args.s3_prefix,
        str(output_dir),
    ]
    print("[download_openalex] running:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    print(f"[download_openalex] synced to: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

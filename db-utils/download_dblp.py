#!/usr/bin/env python3
"""Download the latest DBLP XML dump for CiteFocus."""

from __future__ import annotations

import argparse
import shutil
import tempfile
import urllib.request
from pathlib import Path


ROOT = Path("/home/sascha/refcheck/CiteFocus")
DBLP_DIR = ROOT / "academic_metadata" / "dblp"
DEFAULT_URL = "https://dblp.org/xml/dblp.xml.gz"
DEFAULT_OUTPUT = DBLP_DIR / "dblp.xml.gz"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download the latest DBLP XML dump.")
    parser.add_argument("--url", default=DEFAULT_URL, help="DBLP dump URL")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output path for dblp.xml.gz")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".gz", dir=str(output_path.parent)) as temp_handle:
        temp_path = Path(temp_handle.name)

    try:
        print(f"[download_dblp] downloading {args.url}")
        with urllib.request.urlopen(args.url) as response, temp_path.open("wb") as handle:
            shutil.copyfileobj(response, handle)
        temp_path.replace(output_path)
        print(f"[download_dblp] wrote: {output_path}")
        return 0
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Refresh CiteFocus academic metadata and rebuild local indexes."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path("/home/sascha/refcheck/CiteFocus")
DB_UTILS_DIR = ROOT / "db-utils"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh raw metadata and rebuild CiteFocus local indexes.")
    parser.add_argument("--python", default=sys.executable, help="Python interpreter for db-utils scripts")
    parser.add_argument("--arxiv-rebuild-pages", action="store_true", help="Delete existing arXiv page_*.xml files before redownloading")
    parser.add_argument("--skip-download", action="store_true", help="Skip downloader/update steps")
    parser.add_argument("--skip-index", action="store_true", help="Skip index rebuild steps")
    return parser.parse_args()


def run_command(cmd: list[str]) -> None:
    print("[refresh_metadata_and_indexes] running:", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> int:
    args = parse_args()

    if not args.skip_download:
        arxiv_cmd = [args.python, str(DB_UTILS_DIR / "download_arxiv.py"), "--rebuild-pages"]
        if args.arxiv_rebuild_pages:
            pass
        run_command(arxiv_cmd)
        run_command([args.python, str(DB_UTILS_DIR / "download_dblp.py")])
        run_command([args.python, str(DB_UTILS_DIR / "download_openalex.py")])

    if not args.skip_index:
        run_command([args.python, str(DB_UTILS_DIR / "build_arxiv_index.py"), "--rebuild"])
        run_command([args.python, str(DB_UTILS_DIR / "build_openalex_index.py"), "--rebuild"])
        run_command([args.python, str(DB_UTILS_DIR / "build_dblp_index.py"), "--rebuild"])

    print("[refresh_metadata_and_indexes] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

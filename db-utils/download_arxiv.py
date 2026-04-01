#!/usr/bin/env python3
"""Download arXiv OAI XML pages for CiteFocus."""

from __future__ import annotations

import argparse
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path


ROOT = Path("/home/sascha/refcheck/CiteFocus")
ARXIV_DIR = ROOT / "academic_metadata" / "arxiv" / "oai_pages"
DEFAULT_BASE_URL = "https://export.arxiv.org/oai2"
OAI_NS = {"oai": "http://www.openarchives.org/OAI/2.0/"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download arXiv OAI XML pages.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="arXiv OAI base URL")
    parser.add_argument("--output-dir", default=str(ARXIV_DIR), help="Directory for page_*.xml files")
    parser.add_argument("--sleep-seconds", type=float, default=3.0, help="Sleep between OAI requests")
    parser.add_argument("--rebuild-pages", action="store_true", help="Delete existing page_*.xml files before downloading")
    return parser.parse_args()


def write_response(output_dir: Path, page_number: int, payload: bytes) -> Path:
    path = output_dir / f"page_{page_number:07d}.xml"
    path.write_bytes(payload)
    return path


def fetch_url(url: str) -> bytes:
    with urllib.request.urlopen(url) as response:
        return response.read()


def extract_resumption_token(payload: bytes) -> str | None:
    root = ET.fromstring(payload)
    token_node = root.find(".//oai:resumptionToken", OAI_NS)
    if token_node is None:
        return None
    token = (token_node.text or "").strip()
    return token or None


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.rebuild_pages:
        for xml_file in output_dir.glob("page_*.xml"):
            xml_file.unlink()

    page_number = 1
    token: str | None = None

    while True:
        if token:
            query = urllib.parse.urlencode({"verb": "ListRecords", "resumptionToken": token})
        else:
            query = urllib.parse.urlencode({"verb": "ListRecords", "metadataPrefix": "arXivRaw"})
        url = f"{args.base_url}?{query}"
        print(f"[download_arxiv] fetching page {page_number}: {url}")
        payload = fetch_url(url)
        path = write_response(output_dir, page_number, payload)
        print(f"[download_arxiv] wrote: {path}")

        token = extract_resumption_token(payload)
        if not token:
            break

        page_number += 1
        if args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    print(f"[download_arxiv] finished pages: {page_number}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""DocAtlas server entrypoint (CLI only)."""
from __future__ import annotations

import argparse
from pathlib import Path

from docatlas import (
    azure_config_from_env,
    load_app_config,
    run_pipeline_parallel,
    warn_missing_ocr_deps,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DocAtlas server (CLI)")
    p.add_argument("--input", required=True, help="Input folder")
    p.add_argument("--output", required=True, help="Output folder")
    p.add_argument("--categories", help="Categories separated by semicolons")
    p.add_argument("--config", help="Path to applications config JSON")
    p.add_argument("--app", help="Application name from config")
    p.add_argument("--dry-run", action="store_true", help="Do not call APIs or move files")
    p.add_argument("--no-resume", action="store_true", help="Disable resume cache")
    p.add_argument("--no-ocrmypdf", action="store_true", help="Disable OCRmyPDF and use Tesseract fallback")
    p.add_argument("--workers", type=int, default=4, help="Parallel workers (default: 4)")
    p.add_argument("--embeddings-source", choices=["summary", "full_text", "none"], help="Use summaries, full text, or disable embeddings")
    p.add_argument("--overwrite-excel", action="store_true", help="Overwrite Excel outputs instead of appending")
    p.add_argument("--limit", type=int, help="Process only the first N files (for estimation)")
    p.add_argument("--no-move", action="store_true", help="Do not move files (for estimation)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config) if args.config else Path(__file__).with_name("applications.json")
    app_config = load_app_config(config_path)

    if args.categories:
        categories = [c.strip() for c in args.categories.split(";") if c.strip()]
        app_name = None
    elif args.app and args.app in app_config:
        categories = app_config[args.app]
        app_name = args.app
    else:
        raise ValueError("Provide --categories or a valid --app from config")

    embeddings_source = "summary" if args.embeddings_source is None else args.embeddings_source

    cfg = azure_config_from_env(require_key=not args.dry_run)
    if not args.dry_run:
        if not cfg.chat_api_key:
            raise ValueError("AZURE_CHAT_API_KEY is not set")
        if embeddings_source != "none" and not cfg.embeddings_api_key:
            raise ValueError("AZURE_EMBEDDINGS_API_KEY is not set")
    warn_missing_ocr_deps(not args.no_ocrmypdf)

    run_pipeline_parallel(
        Path(args.input),
        Path(args.output),
        categories,
        cfg,
        args.dry_run,
        not args.no_resume,
        not args.no_ocrmypdf,
        app_name,
        embeddings_source,
        not args.overwrite_excel,
        args.workers,
        args.limit,
        args.no_move,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

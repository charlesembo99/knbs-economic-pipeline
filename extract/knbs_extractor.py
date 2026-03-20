"""
KNBS Economic Data Extractor
=============================
Supports two modes:

  MODE 1 — Remote download (default)
    Pulls Excel files directly from knbs.or.ke.
    Run:  python extract/knbs_extractor.py

  MODE 2 — Local ingest (use when knbs.or.ke is unreachable)
    Place manually downloaded .xlsx files into data/incoming/
    then run:  python extract/knbs_extractor.py --local

    The extractor will validate, stamp, and move them into
    data/raw/ exactly as if they had been downloaded automatically.

Data source : Kenya National Bureau of Statistics (CC-BY 4.0)
Attribution : KNBS: Economic Survey 2025. https://knbs.or.ke
"""

import sys
import json
import hashlib
import logging
import argparse
import shutil
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone



# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parents[1]
RAW_DATA_DIR  = BASE_DIR / "data" / "raw"
INCOMING_DIR  = BASE_DIR / "data" / "incoming"   # drop manual files here
LOGS_DIR      = BASE_DIR / "logs"

# ── Logging ───────────────────────────────────────────────────────────────────
# Console + rotating daily file handler so every run is permanently recorded.
def _setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    log_file = LOGS_DIR / f"extractor_{datetime.now(timezone.utc).strftime('%Y%m%d')}.log"
    fmt      = "%(asctime)s  %(levelname)-8s  %(message)s"
    datefmt  = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("knbs_extractor")
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers if module is imported multiple times
    if logger.handlers:
        return logger

    # Console handler — what you see in the terminal
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter(fmt, datefmt))
    logger.addHandler(ch)

    # File handler — appends to today's log file, persists across runs
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter(fmt, datefmt))
    logger.addHandler(fh)

    logger.info("Log file → %s", log_file)
    return logger

log = _setup_logging()

# ── Source registry ───────────────────────────────────────────────────────────
# Each entry maps a logical name to its remote URL and expected local filename.
# When running in --local mode, drop the file named `local_filename` into
# data/incoming/ before running the extractor.
KNBS_SOURCES = [
    {
        "name":           "cpi_employment_earnings",
        "description":    "CPI, Employment & Earnings — Chapter 3, KNBS Economic Survey 2025",
        "url":            "https://www.knbs.or.ke/wp-content/uploads/2025/05/Chapter-3-Employment-Earnings-and-Consumer-Price-Indices.xlsx",
        "local_filename": "Chapter-3-Employment-Earnings-and-Consumer-Price-Indices.xlsx",
        "file_type":      "xlsx",
    },
    {
        "name":           "core_noncore_inflation",
        "description":    "Core & Non-Core Inflation — Chapter 19, KNBS Economic Survey 2025",
        "url":            "https://www.knbs.or.ke/wp-content/uploads/2025/05/Chapter-19-Core-and-Non-Core-Inflation-Measures-for-Kenya.xlsx",
        "local_filename": "Chapter-19-Core-and-Non-Core-Inflation-Measures-for-Kenya.xlsx",
        "file_type":      "xlsx",
    },
    {
        "name":           "economy_performance",
        "description":    "GDP & Economy Performance — Chapter 2, KNBS Economic Survey 2025",
        "url":            "https://www.knbs.or.ke/wp-content/uploads/2025/05/Chapter-2-Economy-Performance.xlsx",
        "local_filename": "Chapter-2-Economy-Performance.xlsx",
        "file_type":      "xlsx",
    },
    {
        "name":           "international_trade",
        "description":    "International Trade & BOP — Chapter 6, KNBS Economic Survey 2025",
        "url":            "https://www.knbs.or.ke/wp-content/uploads/2025/05/Chapter-6-International-Trade-and-Balance-of-Payments.xlsx",
        "local_filename": "Chapter-6-International-Trade-and-Balance-of-Payments.xlsx",
        "file_type":      "xlsx",
    },
    {
        "name":           "agriculture",
        "description":    "Agricultural Production — Chapter 7, KNBS Economic Survey 2025",
        "url":            "https://www.knbs.or.ke/wp-content/uploads/2025/05/Chapter-7-Agriculture.xlsx",
        "local_filename": "Chapter-7-Agriculture.xlsx",
        "file_type":      "xlsx",
    },
]

REQUEST_TIMEOUT = 60
MAX_RETRIES     = 3


# ── Shared helpers ────────────────────────────────────────────────────────────

def _checksum(path: Path) -> str:
    """Return the MD5 hex-digest of a file."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _already_extracted(source_name: str, new_checksum: str) -> bool:
    """
    Return True if a file with the same MD5 already exists in data/raw/<source_name>/.

    How it works:
      Every successful run writes a _metadata.json sidecar storing the MD5.
      Before processing a new file we scan existing metadata files for that
      dataset and compare checksums.  A match means the source hasn't changed
      since the last run — no point re-processing it.

    This is the idempotency guard: re-running the extractor on an unchanged
    source is a no-op rather than creating duplicate timestamped copies.
    """
    dataset_dir = RAW_DATA_DIR / source_name
    if not dataset_dir.exists():
        return False

    for meta_file in dataset_dir.glob("*_metadata.json"):
        try:
            meta = json.loads(meta_file.read_text())
            if meta.get("md5_checksum") == new_checksum:
                log.info(
                    "  Duplicate — checksum matches previous extraction (%s). Skipping.",
                    meta.get("extracted_at", "unknown date"),
                )
                return True
        except (json.JSONDecodeError, KeyError):
            continue   # corrupt metadata — don't let it block a fresh ingest

    return False


def _validate_xlsx(path: Path) -> tuple[bool, int]:
    try:
        xl    = pd.ExcelFile(path)
        names = xl.sheet_names
        log.info("  Sheets: %s", names)
        df    = pd.read_excel(path, sheet_name=names[0])
        rows  = len(df)
        if rows < 3:
            log.warning("Only %d rows — likely empty or corrupt: %s", rows, path.name)
            return False, 0
        log.info("  Shape: %d rows × %d cols (first sheet)", rows, df.shape[1])
        return True, rows
    except Exception as exc:
        log.error("Validation error — %s: %s", path.name, exc)
        return False, 0


def _save_metadata(dest_dir: Path, source: dict, path: Path,
                   checksum: str, row_count: int, mode: str) -> None:
    meta = {
        "source_name":  source["name"],
        "description":  source["description"],
        "source_url":   source["url"],
        "ingest_mode":  mode,           # "remote" or "local"
        "file_type":    source["file_type"],
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "filename":     path.name,
        "md5_checksum": checksum,
        "row_count":    row_count,
        "license":      "CC-BY 4.0 — Kenya National Bureau of Statistics",
        "attribution":  "KNBS: Economic Survey 2025. https://knbs.or.ke",
    }
    out = dest_dir / f"{source['name']}_metadata.json"
    out.write_text(json.dumps(meta, indent=2))
    log.info("  Metadata → %s", out.name)


def _stamp(name: str, ext: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{name}_{ts}.{ext}"


# ── MODE 1: Remote download ───────────────────────────────────────────────────

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept":   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
        "Referer":  "https://www.knbs.or.ke/",
    })
    return s


def _download(source: dict, session: requests.Session, dest_dir: Path) -> Path | None:
    dest = dest_dir / _stamp(source["name"], source["file_type"])

    for attempt in range(1, MAX_RETRIES + 1):
        log.info("Attempt %d/%d — %s", attempt, MAX_RETRIES, source["name"])
        try:
            r = session.get(source["url"], timeout=REQUEST_TIMEOUT, stream=True)
            r.raise_for_status()
            if "html" in r.headers.get("Content-Type", "").lower():
                log.warning("Got HTML instead of Excel — URL may have changed.")
                return None
            with open(dest, "wb") as fh:
                for chunk in r.iter_content(16384):
                    fh.write(chunk)
            log.info("  Downloaded %.1f KB → %s", dest.stat().st_size / 1024, dest.name)
            return dest
        except requests.exceptions.ConnectionError:
            log.warning("  Connection error (attempt %d)", attempt)
        except requests.exceptions.HTTPError as e:
            log.warning("  HTTP %s", e.response.status_code)
        except requests.exceptions.Timeout:
            log.warning("  Timeout after %ss", REQUEST_TIMEOUT)
        except Exception as e:
            log.error("  Unexpected: %s", e)

    return None


def extract_remote(sources: list) -> dict:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    session = _make_session()
    results = {}

    for source in sources:
        dest_dir = RAW_DATA_DIR / source["name"]
        dest_dir.mkdir(parents=True, exist_ok=True)

        path = _download(source, session, dest_dir)
        if not path:
            log.error("✗ %s — download failed.\n", source["name"])
            continue

        # Deduplication check — skip if identical file was already processed
        checksum = _checksum(path)
        if _already_extracted(source["name"], checksum):
            path.unlink()   # remove the just-downloaded duplicate
            log.info("↷ %s — skipped (no change since last run).\n", source["name"])
            continue

        valid, rows = _validate_xlsx(path)
        if not valid:
            log.error("✗ %s — validation failed.\n", source["name"])
            continue

        _save_metadata(dest_dir, source, path, _checksum(path), rows, "remote")
        results[source["name"]] = path
        log.info("✓ %s\n", source["name"])

    return results


# ── MODE 2: Local ingest ──────────────────────────────────────────────────────

def extract_local(sources: list) -> dict:
    """
    Ingest manually downloaded files from data/incoming/.

    Steps:
      1. Visit each URL below in your browser and download the .xlsx file.
      2. Place all downloaded files into:  data/incoming/
      3. Run:  python extract/knbs_extractor.py --local
    """
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    results = {}

    # Print the download instructions if the folder is empty
    incoming_files = list(INCOMING_DIR.glob("*.xlsx"))
    if not incoming_files:
        print("\n" + "═" * 65)
        print("  data/incoming/ is empty.")
        print("  Download these files from your browser and place them there:\n")
        for s in sources:
            print(f"  [{s['name']}]")
            print(f"  URL : {s['url']}")
            print(f"  Save as: {s['local_filename']}\n")
        print("  Then re-run:  python extract/knbs_extractor.py --local")
        print("═" * 65 + "\n")
        return {}

    log.info("Found %d file(s) in data/incoming/", len(incoming_files))

    for source in sources:
        incoming_path = INCOMING_DIR / source["local_filename"]

        if not incoming_path.exists():
            # Try a fuzzy match — user may have slightly different filename
            matches = list(INCOMING_DIR.glob(f"*{source['name'].replace('_', '*')}*.xlsx"))
            if matches:
                incoming_path = matches[0]
                log.info("Fuzzy match: %s → %s", source["name"], incoming_path.name)
            else:
                log.warning("✗ %s — file not found in data/incoming/. Expected: %s",
                            source["name"], source["local_filename"])
                continue

        dest_dir = RAW_DATA_DIR / source["name"]
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Copy to raw/ with timestamp (preserves the original in incoming/)
        dest_path = dest_dir / _stamp(source["name"], source["file_type"])
        shutil.copy2(incoming_path, dest_path)
        log.info("Copied %s → %s", incoming_path.name, dest_path.name)

        # Deduplication check — skip if identical file was already processed
        checksum = _checksum(dest_path)
        if _already_extracted(source["name"], checksum):
            dest_path.unlink()   # remove the duplicate copy
            log.info("↷ %s — skipped (no change since last run).\n", source["name"])
            continue

        valid, rows = _validate_xlsx(dest_path)
        if not valid:
            log.error("✗ %s — validation failed.\n", source["name"])
            continue

        _save_metadata(dest_dir, source, dest_path, _checksum(dest_path), rows, "local")
        results[source["name"]] = dest_path
        log.info("✓ %s\n", source["name"])

    return results


# ── CLI ───────────────────────────────────────────────────────────────────────

def _print_results(results: dict) -> None:
    print("\n── Extracted successfully ─────────────────────────────────────")
    for name, path in results.items():
        meta_file = path.parent / f"{name}_metadata.json"
        meta = json.loads(meta_file.read_text()) if meta_file.exists() else {}
        print(f"\n  {name}")
        print(f"    File  : {path.name}  ({path.stat().st_size / 1024:.1f} KB)")
        print(f"    Rows  : {meta.get('row_count', '?')}")
        print(f"    Mode  : {meta.get('ingest_mode', '?')}")
        print(f"    MD5   : {meta.get('md5_checksum', '?')[:16]}...")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KNBS data extractor")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Ingest from data/incoming/ instead of downloading from the web",
    )
    args = parser.parse_args()

    mode    = "local" if args.local else "remote"
    sources = KNBS_SOURCES

    log.info("═══ KNBS Extraction — mode: %s — %d source(s) ═══", mode.upper(), len(sources))

    if mode == "local":
        results = extract_local(sources)
    else:
        results = extract_remote(sources)

    if not results:
        log.error("Nothing extracted. See logs above.")
        if mode == "remote":
            print("\nTip: If knbs.or.ke is blocked on your network, try:")
            print("  python extract/knbs_extractor.py --local")
            print("  (follow the instructions it prints to download files manually)\n")
        sys.exit(1)

    _print_results(results)
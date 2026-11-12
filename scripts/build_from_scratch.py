"""
Full bootstrap: ingest 2019-01-01 → today, rebuild all processed and curated
layers, upload to R2, trigger Render redeploy, send email notification.

Usage:
    python scripts/build_from_scratch.py
    python scripts/build_from_scratch.py --skip-ingest     # raw data already present
    python scripts/build_from_scratch.py --skip-upload     # skip R2 + Render step
"""
import argparse
import os
import smtplib
import sys
import urllib.request
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.core.fetch_config import FetchConfig
from src.data.master_combined_gbond import GbondProcessor
from src.data.master_derivatives_fetch import DerivativesFetcher
from src.data.master_index_fetch import MasterIndexFetcher
from src.data.master_index_yield_fetch import MasterIndexYieldFetcher
from src.data.master_lot_size_map import LotSizeMapStore
from src.data.master_trade_calendar_writer import TradeCalendarWriter
from src.data.processed_derivatives_builder import ProcessedDerivativesBuilder
from src.data.processed_gbond_builder import ProcessedGBondBuilder
from src.data.processed_index_spot_builder import ProcessedIndexSpotBuilder
from src.data.processed_index_yield_builder import ProcessedIndexYieldBuilder
from src.data.processed_lot_size_builder import ProcessedLotSizeBuilder
from src.data.processed_trade_calendar_builder import ProcessedTradeCalendarBuilder
from src.data.processed_vix_builder import ProcessedVIXBuilder
from src.data.curated_futures_builder import CuratedFuturesBuilder
from src.data.curated_option_chain_builder import CuratedOptionChainBuilder

START_DATE = date(2019, 1, 1)

_log_lines: list[str] = []
_status = "SUCCESS"


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    _log_lines.append(line)


def send_email(subject_suffix: str = ""):
    notify_email    = os.environ.get("NOTIFY_EMAIL")
    notify_password = os.environ.get("NOTIFY_PASSWORD")
    if not notify_email or not notify_password:
        log("NOTIFY_EMAIL / NOTIFY_PASSWORD not set — skipping email.")
        return

    log_path = BASE_DIR / "logs" / "data_pipeline_fetch.log"
    pipeline_tail = ""
    if log_path.exists():
        lines = log_path.read_text(errors="replace").splitlines()
        pipeline_tail = "\n".join(lines[-100:])

    subject = f"QRL Bootstrap — {_status} — {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    if subject_suffix:
        subject += f" — {subject_suffix}"

    body = (
        "QRL Risk Console Bootstrap Report\n"
        "===================================\n"
        f"Status: {_status}\n\n"
        "--- SUMMARY ---\n"
        + "\n".join(_log_lines)
        + "\n\n--- PIPELINE LOG (last 100 lines) ---\n"
        + pipeline_tail
    )

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"]    = notify_email
    msg["To"]      = notify_email
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(notify_email, notify_password)
            server.send_message(msg)
        log("Email sent.")
    except Exception as exc:
        log(f"Email send failed: {exc}")


def trigger_render_deploy():
    hook = os.environ.get("RENDER_DEPLOY_HOOK")
    if not hook:
        log("RENDER_DEPLOY_HOOK not set — skipping Render redeploy.")
        return
    try:
        with urllib.request.urlopen(
            urllib.request.Request(hook, method="POST", data=b"")
        ) as resp:
            log(f"Render deploy triggered — HTTP {resp.status}")
    except Exception as exc:
        log(f"Render deploy failed: {exc}")


def run_ingest(config: FetchConfig, end_date: date):
    log("--- Step 1: Ingest ---")

    log("  Trade calendar …")
    TradeCalendarWriter(config, rebuild=True).run()

    log("  Lot-size map …")
    LotSizeMapStore(config=config).build()

    log(f"  Derivatives {START_DATE} → {end_date} …")
    failed = DerivativesFetcher(config, rebuild=True).run(START_DATE, end_date)
    if failed:
        log(f"  WARNING: {len(failed)} derivative dates failed: {failed}")

    log(f"  Index spot {START_DATE} → {end_date} …")
    MasterIndexFetcher(config, rebuild=True).run(START_DATE, end_date)

    log(f"  Index yield {START_DATE} → {end_date} …")
    failed_yield = MasterIndexYieldFetcher(config, rebuild=True).run(START_DATE, end_date)
    if failed_yield:
        log(f"  WARNING: {len(failed_yield)} yield dates failed: {failed_yield}")

    log("  G-bond combined …")
    GbondProcessor(config).build_combined_gbond()

    log("--- Ingest complete ---")


def run_processed(config: FetchConfig):
    log("--- Step 2: Processed layer (full mode) ---")
    processors = [
        ("trade_calendar", ProcessedTradeCalendarBuilder),
        ("lot_size",       ProcessedLotSizeBuilder),
        ("derivatives",    ProcessedDerivativesBuilder),
        ("index_spot",     ProcessedIndexSpotBuilder),
        ("vix",            ProcessedVIXBuilder),
        ("index_yield",    ProcessedIndexYieldBuilder),
        ("gbond",          ProcessedGBondBuilder),
    ]
    for name, Cls in processors:
        log(f"  {name} …")
        Cls(config).run("full")
    log("--- Processed complete ---")


def run_curated(config: FetchConfig):
    log("--- Step 3: Curated layer (full mode) ---")
    log("  option_chain …")
    CuratedOptionChainBuilder(config).run("full")
    log("  futures …")
    CuratedFuturesBuilder(config).run("full")
    log("--- Curated complete ---")


def run_upload():
    log("--- Step 4: Upload to R2 ---")
    upload_script = BASE_DIR / "scripts" / "upload_to_r2.py"
    import subprocess
    result = subprocess.run(
        [sys.executable, str(upload_script)],
        capture_output=True, text=True,
    )
    if result.stdout:
        log(result.stdout.strip())
    if result.returncode != 0:
        raise RuntimeError(f"upload_to_r2 failed:\n{result.stderr}")
    log("--- Upload complete ---")


def main():
    global _status

    parser = argparse.ArgumentParser(description="Full QRL bootstrap from 2019-01-01")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Skip ingest (raw data already present)")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Skip R2 upload and Render redeploy")
    args = parser.parse_args()

    end_date = date.today()
    config   = FetchConfig(BASE_DIR, use_year_partition=True)

    log(f"=== QRL bootstrap START — {START_DATE} → {end_date} ===")

    try:
        if not args.skip_ingest:
            run_ingest(config, end_date)
        else:
            log("Ingest skipped (--skip-ingest).")

        run_processed(config)
        run_curated(config)

        if not args.skip_upload:
            run_upload()
            trigger_render_deploy()
        else:
            log("Upload + Render skipped (--skip-upload).")

        log("=== Bootstrap COMPLETE ===")

    except Exception as exc:
        _status = "FAILED"
        log(f"FATAL: {exc}")
        send_email()
        sys.exit(1)

    send_email()


if __name__ == "__main__":
    main()

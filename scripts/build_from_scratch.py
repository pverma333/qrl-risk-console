"""
Full bootstrap: ingest 2019-01-01 → today, rebuild all processed and curated
layers, run sync check, upload to R2, trigger Render redeploy, send email.

Equivalent to running these commands in sequence:
    python -m scripts.run_data_pipeline --start 2019-01-01 --end <today> --rebuild
    python scripts/run_processed_builder.py --mode full
    python scripts/run_curated_option_chain.py --mode full
    python scripts/run_curated_futures.py --mode full
    python -m scripts.check_sync_status

Usage:
    python scripts/build_from_scratch.py
    python scripts/build_from_scratch.py --skip-ingest     # raw data already present
    python scripts/build_from_scratch.py --skip-upload     # skip R2 + Render step
"""
import argparse
import os
import smtplib
import subprocess
import sys
import urllib.request
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

START_DATE = date(2019, 1, 1)

_log_lines: list[str] = []
_status = "SUCCESS"


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    _log_lines.append(line)


def run_step(label: str, cmd: list[str]):
    """Run a subprocess step, streaming output and raising on failure."""
    log(f"--- {label} ---")
    log(f"  cmd: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        cwd=BASE_DIR,
        capture_output=False,   # let stdout/stderr stream to console
    )
    if result.returncode != 0:
        raise RuntimeError(f"{label} exited with code {result.returncode}")
    log(f"--- {label} complete ---")


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


def main():
    global _status

    parser = argparse.ArgumentParser(description="Full QRL bootstrap from 2019-01-01")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="Skip ingest (raw data already present)")
    parser.add_argument("--skip-upload", action="store_true",
                        help="Skip R2 upload and Render redeploy")
    args = parser.parse_args()

    end_date = date.today()
    py = sys.executable

    log(f"=== QRL bootstrap START — {START_DATE} → {end_date} ===")

    try:
        # Step 1 — Ingest
        if not args.skip_ingest:
            run_step(
                "Step 1: Ingest (run_data_pipeline)",
                [py, "-m", "scripts.run_data_pipeline",
                 "--start", str(START_DATE),
                 "--end",   str(end_date),
                 "--rebuild"],
            )
        else:
            log("Step 1: Ingest skipped (--skip-ingest).")

        # Step 2 — Processed layer
        run_step(
            "Step 2: Processed layer (run_processed_builder --mode full)",
            [py, "scripts/run_processed_builder.py", "--mode", "full"],
        )

        # Step 3 — Curated option chain
        run_step(
            "Step 3: Curated option chain (run_curated_option_chain --mode full)",
            [py, "scripts/run_curated_option_chain.py", "--mode", "full"],
        )

        # Step 4 — Curated futures
        run_step(
            "Step 4: Curated futures (run_curated_futures --mode full)",
            [py, "scripts/run_curated_futures.py", "--mode", "full"],
        )

        # Step 5 — Sync check
        run_step(
            "Step 5: Sync check (check_sync_status)",
            [py, "-m", "scripts.check_sync_status"],
        )

        # Step 6 — Upload + deploy
        if not args.skip_upload:
            run_step(
                "Step 6: Upload to R2",
                [py, "scripts/upload_to_r2.py"],
            )
            trigger_render_deploy()
        else:
            log("Step 6: Upload + Render skipped (--skip-upload).")

        log("=== Bootstrap COMPLETE ===")

    except Exception as exc:
        _status = "FAILED"
        log(f"FATAL: {exc}")
        send_email()
        sys.exit(1)

    send_email()


if __name__ == "__main__":
    main()

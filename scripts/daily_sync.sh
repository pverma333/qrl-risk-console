#!/bin/bash

source /Users/priyamverma/.qrl_env

cd /Users/priyamverma/Documents/GitHub/qrl-risk-console

LOG=""
STATUS="SUCCESS"
DATE=$(date)

log() {
    echo "$1"
    LOG="$LOG\n$1"
}

send_email() {
    SUBJECT="QRL Daily Sync — $STATUS — $DATE"
    BODY="QRL Risk Console Daily Pipeline Report\n"
    BODY="$BODY=====================================\n"
    BODY="$BODY Date: $DATE\n"
    BODY="$BODY Status: $STATUS\n\n"
    BODY="$BODY$LOG"

    python3 -c "
import smtplib
from email.mime.text import MIMEText
import os

msg = MIMEText('$BODY')
msg['Subject'] = '$SUBJECT'
msg['From'] = os.environ.get('NOTIFY_EMAIL')
msg['To'] = os.environ.get('NOTIFY_EMAIL')

with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
    server.login(os.environ.get('NOTIFY_EMAIL'), os.environ.get('NOTIFY_PASSWORD'))
    server.send_message(msg)
print('Email sent.')
"
}

log "=== Stopping local uvicorn if running $DATE ==="
pkill -f "uvicorn app.main:app" 2>/dev/null
sleep 3

log "=== Starting daily fetch $(date) ==="
/opt/anaconda3/bin/python -m scripts.run_daily_fetch
if [ $? -ne 0 ]; then
    log "ERROR: run_daily_fetch FAILED."
    STATUS="FAILED — Step 1: run_daily_fetch"
    send_email
    exit 1
fi
log "=== Fetch complete $(date) ==="

log "=== Uploading to R2 $(date) ==="
/opt/anaconda3/bin/python scripts/upload_to_r2.py
if [ $? -ne 0 ]; then
    log "ERROR: upload_to_r2 FAILED."
    STATUS="FAILED — Step 2: upload_to_r2"
    send_email
    exit 1
fi
log "=== Upload complete $(date) ==="

log "=== Triggering Render redeploy $(date) ==="
DEPLOY_RESPONSE=$(curl -s -X POST "$RENDER_DEPLOY_HOOK")
log "Render response: $DEPLOY_RESPONSE"
log "=== Redeploy triggered $(date) ==="

log "=== Restarting local uvicorn $(date) ==="
nohup uvicorn app.main:app --reload > /tmp/uvicorn.log 2>&1 &
log "Local uvicorn started. PID: $!"

log "=== All steps complete $(date) ==="
send_email

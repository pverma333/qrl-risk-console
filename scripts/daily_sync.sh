#!/bin/bash

source /Users/priyamverma/.qrl_env

cd /Users/priyamverma/Documents/GitHub/qrl-risk-console

echo "=== Starting daily fetch $(date) ==="

/opt/anaconda3/bin/python -m scripts.run_daily_fetch
if [ $? -ne 0 ]; then
    echo "ERROR: run_daily_fetch failed. Aborting."
    exit 1
fi

echo "=== Fetch complete. Uploading to R2 $(date) ==="

/opt/anaconda3/bin/python scripts/upload_to_r2.py
if [ $? -ne 0 ]; then
    echo "ERROR: upload_to_r2 failed. Aborting."
    exit 1
fi

echo "=== Upload complete. Triggering Render redeploy $(date) ==="

curl -X POST "$RENDER_DEPLOY_HOOK"

echo "=== Done $(date) ==="

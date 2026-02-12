#!/usr/bin/env bash
set -u

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARENT_DIR="$(dirname "$DIR")"
cd "$PARENT_DIR"

export PYTHONUNBUFFERED=1
export PYTHONPATH="$PARENT_DIR"

echo "Starting continuous refill loop at $(date)"

while true; do
    echo "----------------------------------------------------------------"
    echo "Launching refill process (Full Universe)..."
    
    # Removed --limit 50 to process all remaining stocks in one go
    PYBIN="$PARENT_DIR/.venv/bin/python"
    if [ ! -x "$PYBIN" ]; then
        PYBIN="python3"
    fi

    $PYBIN -u -m src.collectors.refill_loader \
      --chunk-days 150 \
      --start-mode listing \
      --sleep 0.1 \
      --resume

    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo "Refill process finished successfully."
        # If finished successfully, it likely means all stocks are DONE.
        # We can stop the loop or sleep for a long time.
        echo "All done? Checking DB..."
        REMAINING=$(/usr/bin/sqlite3 data/market_data.db "SELECT count(*) FROM refill_progress WHERE status != 'DONE';")
        if [ "$REMAINING" -eq "0" ]; then
             echo "All stocks are DONE. Exiting loop."
             break
        fi
    else
        echo "Refill process crashed with code $EXIT_CODE."
    fi

    echo "Restarting in 10 seconds..."
    sleep 10
done

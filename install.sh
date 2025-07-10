#!/bin/sh

set -e

echo "[Plugin Install] Starting post-installation script."

if [ -z "$DIP_HOME" ]; then
    echo "[Plugin Install] ERROR: DIP_HOME environment variable is not set. Cannot proceed." >&2
    exit 1
fi

HYPERD_BINARY_PATH="$DIP_HOME/java-lib/hyper/hyperd"

echo "[Plugin Install] Checking for binary at: $HYPERD_BINARY_PATH"

if [ -f "$HYPERD_BINARY_PATH" ]; then
    chmod a+x "$HYPERD_BINARY_PATH"
    echo "[Plugin Install] Successfully set execute permission on the hyperd binary."
else
    echo "[Plugin Install] WARNING: The hyperd binary was not found. Skipping permission change." >&2
fi

echo "[Plugin Install] Post-installation script finished."
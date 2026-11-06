#!/bin/bash
set -e

# Cleanup any leftover X server locks and ensure runtime dirs exist
rm -f /tmp/.X99-lock /tmp/.X11-unix/X99 2>/dev/null || true
mkdir -p /app/downloads /app/local_profiles /app/scratch 2>/dev/null || true

# Start Xvfb virtual frame buffer on display :99
Xvfb :99 -screen 0 1280x800x24 -ac +extension GLX +render -noreset &
export DISPLAY=:99

# Wait up to 5s for Xvfb display socket to be ready
for i in $(seq 1 10); do
    if [ -e /tmp/.X11-unix/X99 ]; then
        break
    fi
    sleep 0.5
done

# Start Openbox window manager
openbox >/dev/null 2>&1 &

# Start x11vnc server on display :99
x11vnc -display :99 -forever -shared -nopw -rfbport 5900 -bg || true

# Ensure default index.html exists for noVNC
ln -sf /usr/share/novnc/vnc.html /usr/share/novnc/index.html 2>/dev/null || true

# Start noVNC websockify on port 6080
websockify --web /usr/share/novnc 6080 localhost:5900 >/dev/null 2>&1 &

echo "=========================================================="
echo "🚀 Playwright Automation Server running on port 3000"
echo "🖥️  Interactive Browser Screen (noVNC): http://localhost:6080"
echo "=========================================================="

# Start Node.js server
exec node server.js

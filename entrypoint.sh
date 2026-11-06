#!/bin/bash
set -e

# Cleanup any leftover X server locks
rm -f /tmp/.X99-lock /tmp/.X11-unix/X99 2>/dev/null || true

# Start Xvfb virtual frame buffer on display :99
Xvfb :99 -screen 0 1280x800x24 -ac +extension GLX +render -noreset &
export DISPLAY=:99
sleep 1

# Start Openbox window manager
openbox &

# Start x11vnc server on display :99
x11vnc -display :99 -forever -shared -nopw -rfbport 5900 -bg

# Ensure default index.html exists for noVNC
ln -sf /usr/share/novnc/vnc.html /usr/share/novnc/index.html 2>/dev/null || true

# Start noVNC websockify on port 6080
websockify --web /usr/share/novnc 6080 localhost:5900 &

echo "=========================================================="
echo "🚀 Playwright Automation Server running on port 3000"
echo "🖥️  Interactive Browser Screen (noVNC): http://localhost:6080"
echo "=========================================================="

# Start Node.js server
exec node server.js

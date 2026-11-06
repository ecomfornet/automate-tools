# ==============================================================================
# Playwright Automation Tool — Production Dockerfile with noVNC Web GUI
# ==============================================================================
FROM mcr.microsoft.com/playwright:v1.61.1-jammy

# Install GUI display server, window manager, and web VNC stack
RUN apt-get update -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        xvfb \
        x11vnc \
        novnc \
        websockify \
        openbox \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy package definitions
COPY package*.json ./

# Install production dependencies
RUN npm install --omit=dev

# Verify and ensure Playwright Chromium browser & headless shell are installed
RUN npx playwright install chromium chromium-headless-shell || npx playwright install chromium

# Copy all application code
COPY . .

# Normalize line endings and ensure entrypoint is executable
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Ensure storage and profile directories exist
RUN mkdir -p /app/downloads /app/local_profiles /app/scratch

# Set default environment variables
ENV NODE_ENV=production \
    PORT=3000 \
    DISPLAY=:99 \
    RESOLUTION=1920x1080x24 \
    VNC_PORT=5900 \
    NOVNC_PORT=6080 \
    BROWSER_DATA_DIR=/app/local_profiles \
    DOWNLOADS_DIR=/app/downloads \
    SCRATCH_DIR=/app/scratch

# Expose HTTP API/Web UI and noVNC web interface
EXPOSE 3000 6080

# Health check against Express server
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:3000/api/health || exit 1

# Start virtual display, window manager, VNC server, noVNC proxy, and application
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["node", "server.js"]

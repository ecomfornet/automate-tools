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
RUN npx playwright install-deps firefox || true
RUN npx camoufox fetch || true

# Copy all application code
COPY . .

# Normalize line endings and ensure entrypoint is executable
RUN sed -i 's/\r$//' /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Ensure storage and profile directories exist
RUN mkdir -p /app/downloads /app/local_profiles /app/scratch

ENV NODE_ENV=production \
    PORT=3000 \
    CONCURRENCY=5 \
    DISPLAY=:99

EXPOSE 3000 6080

# Health check verifies the server and in-memory queue are responsive
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD node -e "fetch('http://localhost:3000/admin/queues').then(r => r.ok ? process.exit(0) : process.exit(1)).catch(() => process.exit(1))"

ENTRYPOINT ["/app/entrypoint.sh"]

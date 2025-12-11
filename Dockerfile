# Dockerfile (for Selenium + Chromium + Gunicorn)
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver \
    PORT=8000

# Install Chromium, Chromedriver and required libraries
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      wget \
      ca-certificates \
      curl \
      unzip \
      gnupg \
      fonts-liberation \
      fonts-dejavu-core \
      libnss3 \
      libnspr4 \
      libx11-6 \
      libx11-xcb1 \
      libxcb1 \
      libxcomposite1 \
      libxdamage1 \
      libxext6 \
      libxfixes3 \
      libxrandr2 \
      libxrender1 \
      libxss1 \
      libatk1.0-0 \
      libatk-bridge2.0-0 \
      libcups2 \
      libdrm2 \
      libgbm1 \
      libgtk-3-0 \
      lsb-release \
      sudo \
      ca-certificates; \
    \
    # Install Chromium and chrome-driver from Debian repo (compatible with this image)
    apt-get install -y --no-install-recommends chromium chromium-driver; \
    \
    # Ensure the binaries exist
    if [ ! -x /usr/bin/chromium ] && [ -x /usr/bin/chromium-browser ]; then \
        ln -s /usr/bin/chromium-browser /usr/bin/chromium; \
    fi; \
    \
    # Clean apt caches
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install python deps
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Copy application code
COPY . .

EXPOSE 8000

# Use gunicorn to serve your app. We keep a slightly longer timeout for scrapers.
CMD ["bash", "-c", "gunicorn app:app --bind 0.0.0.0:${PORT:-8000} --workers 2 --threads 4 --timeout 120"]

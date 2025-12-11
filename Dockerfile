FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver \
    PORT=8000

# Install system dependencies for both Selenium and Playwright
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      chromium \
      chromium-driver \
      ca-certificates \
      curl \
      wget \
      gnupg \
      libnss3 \
      libnspr4 \
      libatk1.0-0 \
      libatk-bridge2.0-0 \
      libcups2 \
      libdrm2 \
      libdbus-1-3 \
      libxkbcommon0 \
      libxcomposite1 \
      libxdamage1 \
      libxfixes3 \
      libxrandr2 \
      libgbm1 \
      libasound2 \
      libpango-1.0-0 \
      libcairo2 \
      libatspi2.0-0 \
      libxshmfence1 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Install Playwright browsers (system deps already installed above)
RUN playwright install chromium

COPY . .

EXPOSE 8000

CMD ["bash", "-c", "gunicorn app:app --bind 0.0.0.0:${PORT:-8000} --workers 2 --threads 4 --timeout 120"]


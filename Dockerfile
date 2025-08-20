# Simple runtime image for SolSniper
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (if any lightweight ones are needed later)
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    curl ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy and install python deps first for better layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app
COPY . .

# Default command (expects env via secrets)
CMD ["python", "run_bot.py"]
